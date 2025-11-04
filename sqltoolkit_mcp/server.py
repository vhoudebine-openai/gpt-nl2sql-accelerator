"""FastMCP server exposing sqltoolkit-powered SQL utilities."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional, Type

from dotenv import load_dotenv
from fastmcp import FastMCP

from sqltoolkit import (
    AzureSQLConnector,
    DatabaseClient,
    DatabricksConnector,
    OdbcConnector,
    PostgreSQLConnector,
    SnowflakeConnector,
)

load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

mcp = FastMCP(
    name="SQLToolkitServer",
    instructions=(
        "Execute read-only SQL operations through the sqltoolkit library. "
        "Configure SQL_CONNECTOR_CONFIG with the connector type and parameters."
    ),
)

_CONFIG: Dict[str, Any] = {}
_CLIENT: Optional[DatabaseClient] = None


def _load_config() -> Dict[str, Any]:
    global _CONFIG
    if _CONFIG:
        return _CONFIG

    config_raw = os.getenv("SQL_CONNECTOR_CONFIG")
    if not config_raw:
        raise RuntimeError("SQL_CONNECTOR_CONFIG environment variable is not set.")

    try:
        _CONFIG = json.loads(config_raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("SQL_CONNECTOR_CONFIG must be valid JSON.") from exc

    return _CONFIG


def _connector_from_config(config: Dict[str, Any]):
    connector_type = (config.get("type") or "").upper()
    if not connector_type:
        raise ValueError("Connector configuration must include a 'type' field.")

    connector_map: Dict[str, Type] = {
        "AZURE_SQL": AzureSQLConnector,
        "POSTGRESQL": PostgreSQLConnector,
        "ODBC": OdbcConnector,
        "SNOWFLAKE": SnowflakeConnector,
        "DATABRICKS": DatabricksConnector,
    }

    connector_cls = connector_map.get(connector_type)
    if connector_cls is None:
        raise ValueError(f"Unsupported connector type '{connector_type}'.")

    kwargs = {k: v for k, v in config.items() if k != "type"}
    return connector_cls(**kwargs)


def _get_client() -> DatabaseClient:
    global _CLIENT
    if _CLIENT is None:
        config = _load_config()
        connector = _connector_from_config(config)
        _CLIENT = DatabaseClient(connector)
    return _CLIENT


def _reset_client():
    global _CLIENT
    _CLIENT = None


def _frame_response(df, *, limit: Optional[int] = None) -> str:
    if limit is not None and limit > 0:
        limited_df = df.head(limit)
        limited = len(df.index) > len(limited_df.index)
        df = limited_df
    else:
        limited = False

    rows = df.to_dict(orient="records")
    payload = {
        "rowCount": len(df.index),
        "rows": rows,
        "limited": limited,
    }
    return json.dumps(payload)


@mcp.tool
def list_tables() -> str:
    """Return the available tables for the configured connector."""
    try:
        client = _get_client()
        return client.list_database_tables()
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Failed to list tables: %s", exc)
        _reset_client()
        return f"Failed to list tables: {exc}"


@mcp.tool
def table_schema(table_name: str) -> str:
    """Return column metadata for the specified table."""
    if not table_name:
        return "Provide a table name."

    try:
        client = _get_client()
        return client.get_table_schema(table_name)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Failed to fetch schema for %s: %s", table_name, exc)
        _reset_client()
        return f"Failed to fetch schema for {table_name}: {exc}"


@mcp.tool
def query_sql(sql: str, limit: int = 500) -> str:
    """Execute a read-only SQL query and return JSON rows."""
    if not sql or not sql.strip():
        return "SQL query is empty. Provide a valid SQL statement."

    try:
        client = _get_client()
        df = client._read_sql(sql)  # pylint: disable=protected-access
        df = client.convert_datetime_columns_to_string(df)
        return _frame_response(df, limit=limit)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("SQL query failed: %s", exc)
        _reset_client()
        return f"Query failed: {exc}"


@mcp.tool
def column_values(table_name: str, column_name: str) -> str:
    """Return distinct column values to help with filter construction."""
    if not table_name or not column_name:
        return "Provide both table_name and column_name."

    try:
        client = _get_client()
        return client.get_column_values(table_name, column_name)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception(
            "Failed to fetch column values for %s.%s: %s", table_name, column_name, exc
        )
        _reset_client()
        return f"Failed to fetch column values: {exc}"


if __name__ == "__main__":
    mcp.run(transport="http", host="127.0.0.1", port=9000)
