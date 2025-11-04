import azure.functions as func
import base64
import io
import json
import logging
import os
import uuid
from typing import Any, Dict, Optional

from sqltoolkit import (
    DatabaseClient,
    AzureSQLConnector,
    PostgreSQLConnector,
    OdbcConnector,
    SnowflakeConnector,
    DatabricksConnector,
)

app = func.FunctionApp()

_CONNECTOR_CONFIG_RAW = os.environ.get("SQL_CONNECTOR_CONFIG")
_CONNECTOR_CONFIG: Dict[str, Any] = {}
if _CONNECTOR_CONFIG_RAW:
    try:
        _CONNECTOR_CONFIG = json.loads(_CONNECTOR_CONFIG_RAW)
    except json.JSONDecodeError as exc:
        logging.error("Failed to parse SQL_CONNECTOR_CONFIG: %s", exc)

_SQL_CLIENT: Optional[DatabaseClient] = None


def _build_connector(config: Dict[str, Any]):
    connector_type = (config.get("type") or "").upper()
    if not connector_type:
        raise ValueError("Connector configuration must include a 'type' key.")

    connector_map = {
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


def _get_sql_client() -> DatabaseClient:
    global _SQL_CLIENT
    if _SQL_CLIENT is None:
        if not _CONNECTOR_CONFIG:
            raise RuntimeError("SQL_CONNECTOR_CONFIG environment variable is not set or invalid.")
        connector = _build_connector(_CONNECTOR_CONFIG)
        _SQL_CLIENT = DatabaseClient(connector)
    return _SQL_CLIENT


def _reset_client():
    global _SQL_CLIENT
    _SQL_CLIENT = None


@app.route(route="sql/tables", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def list_tables(req: func.HttpRequest) -> func.HttpResponse:
    try:
        client = _get_sql_client()
        tables_json = client.list_database_tables()
        return func.HttpResponse(tables_json, mimetype="application/json")
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("Failed to list tables: %s", exc)
        _reset_client()
        return func.HttpResponse(str(exc), status_code=500)


@app.route(route="sql/schema/{table_name}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_schema(req: func.HttpRequest) -> func.HttpResponse:
    table_name = req.route_params.get("table_name")
    if not table_name:
        return func.HttpResponse("'table_name' is required in the route.", status_code=400)

    try:
        client = _get_sql_client()
        schema_json = client.get_table_schema(table_name)
        return func.HttpResponse(schema_json, mimetype="application/json")
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("Failed to fetch schema: %s", exc)
        _reset_client()
        return func.HttpResponse(str(exc), status_code=500)


@app.route(route="sql/query", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def run_query(req: func.HttpRequest) -> func.HttpResponse:
    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON body.", status_code=400)

    query = (payload or {}).get("query")
    if not query:
        return func.HttpResponse("'query' must be provided in the request body.", status_code=400)

    desired_filename = (payload or {}).get("filename")

    try:
        client = _get_sql_client()
        df = client._read_sql(query)  # pylint: disable=protected-access
        df = client.convert_datetime_columns_to_string(df)
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("SQL query failed: %s", exc)
        _reset_client()
        return func.HttpResponse(str(exc), status_code=500)

    row_count = len(df.index)
    response_payload = {
        "rowCount": row_count,
        "rows": [],
        "openaiFileResponse": [],
    }

    if row_count > 10:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        csv_bytes = buffer.getvalue().encode("utf-8")
        buffer.close()

        encoded_csv = base64.b64encode(csv_bytes).decode("utf-8")
        filename = desired_filename or f"query_result_{uuid.uuid4().hex}.csv"

        response_payload["openaiFileResponse"].append(encoded_csv)
        response_payload["fileName"] = filename
        response_payload["mimeType"] = "text/csv"
    else:
        response_payload["rows"] = df.to_dict(orient="records")

    return func.HttpResponse(json.dumps(response_payload), mimetype="application/json", status_code=200)
