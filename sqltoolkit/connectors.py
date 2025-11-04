# my_module/sample_class.py
import struct
import time
from typing import List, Optional

import pandas as pd
import pyodbc
import requests
import snowflake.connector
from azure.identity import DefaultAzureCredential
import psycopg2
from psycopg2 import OperationalError


class AzureSQLConnector:
    def __init__(self, server: str, database: str, use_entra_id: bool = True, username: str = None, password: str = None):
        self.type = 'AZURE_SQL'
        self.use_entra_id = use_entra_id
        if use_entra_id:
            self.connection_string = f'Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{server},1433;Database={database};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
        else:
            if not username or not password:
                raise ValueError("Username and password must be provided for user password authentication.")
            self.connection_string = f'Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{server},1433;Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

    def get_conn(self):
        try:
            if self.use_entra_id:
                credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
                token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
                token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
                SQL_COPT_SS_ACCESS_TOKEN = 1256
                conn = pyodbc.connect(self.connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
            else:
                conn = pyodbc.connect(self.connection_string)
            return conn
        except pyodbc.Error as e:
            raise RuntimeError(f"Error connecting to Azure SQL Database: {e}")
        
class PostgreSQLConnector:
    def __init__(self, host: str, database: str, username: str, password: str, port: int = 5432):
        self.type = 'POSTGRESQL'
        self.connection_string = f"dbname='{database}' user='{username}' password='{password}' host='{host}' port='{port}'"

    def get_conn(self):
        try:
            conn = psycopg2.connect(self.connection_string)
            return conn
        except OperationalError as e:
            raise RuntimeError(f"Error connecting to PostgreSQL Database: {e}")

class OdbcConnector:
    def __init__(self, connection_string: str):
        self.type = 'ODBC'
        self.connection_string = connection_string

    def get_conn(self):
        try:
            conn = pyodbc.connect(self.connection_string)
            return conn
        except pyodbc.Error as e:
            raise RuntimeError(f"Error connecting to Database: {e}")

class SnowflakeConnector:
    def __init__(self, user: str, password: str, account: str, warehouse: str, database: str, schema: str, role: str = None, **kwargs):
        """
        Initialize a Snowflake connector.
        
        Required parameters:
          - user: Snowflake username.
          - password: Snowflake password.
          - account: Snowflake account identifier.
          - warehouse: Warehouse name.
          - database: Database name.
          - schema: Schema name.
        
        Optional:
          - role: Snowflake role.
          - kwargs: Additional parameters for snowflake.connector.connect.
        """
        self.type = 'SNOWFLAKE'
        self.connection_params = {
            'user': user,
            'password': password,
            'account': account,
            'warehouse': warehouse,
            'database': database,
            'schema': schema,
        }
        if role:
            self.connection_params['role'] = role
        # Include any additional optional parameters
        self.connection_params.update(kwargs)
    
    def get_conn(self):
        """
        Establish and return a connection to Snowflake.
        """
        try:
            conn = snowflake.connector.connect(**self.connection_params)
            return conn
        except Exception as e:
            raise RuntimeError(f"Error connecting to Snowflake: {e}")


class DatabricksSQLConnection:
    """
    Lightweight connection object that proxies Databricks SQL Statement Execution
    responses into a pandas DataFrame so the rest of the toolkit can stay unchanged.
    """

    def __init__(
        self,
        host: str,
        token: str,
        warehouse_id: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        wait_timeout: str = "40s",
        poll_interval: float = 2.0,
        max_poll_attempts: Optional[int] = 120,
        request_timeout: int = 60,
    ):
        if not host.startswith("http://") and not host.startswith("https://"):
            host = f"https://{host}"
        self.host = host.rstrip("/")
        self.token = token
        self.warehouse_id = warehouse_id
        self.catalog = catalog
        self.schema = schema
        self.wait_timeout = wait_timeout
        self.poll_interval = poll_interval
        self.max_poll_attempts = max_poll_attempts
        self.request_timeout = request_timeout

    def cursor(self):
        return DatabricksCursor(self)

    def close(self):
        """No persistent connection to close."""

    def run_query(self, statement: str) -> pd.DataFrame:
        final_payload = self._execute_statement(statement)
        result = final_payload.get("result") or {}
        schema = result.get("manifest", {}).get("schema") or []
        columns = [col.get("name") for col in schema]
        data_array = result.get("data_array") or []
        if not columns:
            # If Databricks returns an empty schema, we still produce an empty DataFrame.
            return pd.DataFrame()
        return pd.DataFrame(data_array, columns=columns)

    def execute(self, statement: str):
        """Helper so pandas.io.sql can call connection.execute directly."""
        cursor = self.cursor()
        cursor.execute(statement)
        return cursor

    # Internal helpers --------------------------------------------------
    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _execute_statement(self, statement: str) -> dict:
        initial = self._submit_statement(statement)
        status = (initial.get("status") or {}).get("state")
        if status == "SUCCEEDED":
            return initial

        statement_id = initial.get("statement_id")
        if not statement_id:
            raise RuntimeError("Databricks response missing statement_id.")

        return self._poll_for_completion(statement_id)

    def _submit_statement(self, statement: str) -> dict:
        payload = {
            "statement": statement,
            "warehouse_id": self.warehouse_id,
            "wait_timeout": self.wait_timeout,
            "on_wait_timeout": "CONTINUE",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        }
        if self.catalog:
            payload["catalog"] = self.catalog
        if self.schema:
            payload["schema"] = self.schema

        response = requests.post(
            f"{self.host}/api/2.0/sql/statements",
            headers=self._headers(),
            json=payload,
            timeout=self.request_timeout,
        )
        response.raise_for_status()
        return response.json()

    def _poll_for_completion(self, statement_id: str) -> dict:
        attempts = 0
        while True:
            response = requests.get(
                f"{self.host}/api/2.0/sql/statements/{statement_id}",
                headers=self._headers(),
                timeout=self.request_timeout,
            )
            response.raise_for_status()
            payload = response.json()
            status = (payload.get("status") or {}).get("state")

            if status == "SUCCEEDED":
                return payload
            if status in {"FAILED", "CANCELED"}:
                message = (
                    (payload.get("status") or {}).get("error") or {}
                ).get("message", "Unknown Databricks error.")
                raise RuntimeError(f"Databricks query failed: {message}")

            attempts += 1
            if self.max_poll_attempts is not None and attempts >= self.max_poll_attempts:
                raise RuntimeError("Timed out waiting for Databricks query to finish.")
            time.sleep(self.poll_interval)


class DatabricksCursor:
    """
    Minimal DB-API compatible cursor so pandas.read_sql can operate on the REST results.
    """

    def __init__(self, connection: DatabricksSQLConnection):
        self.connection = connection
        self._df: Optional[pd.DataFrame] = None
        self.description: Optional[List[tuple]] = None

    def execute(self, statement: str):
        self._df = self.connection.run_query(statement)
        columns = list(self._df.columns) if self._df is not None else []
        self.description = [(col, None, None, None, None, None, None) for col in columns]
        return self

    def fetchall(self):
        if self._df is None or self._df.empty:
            return []
        return [tuple(row) for row in self._df.itertuples(index=False, name=None)]

    def close(self):
        self._df = None
        self.description = None


class DatabricksConnector:
    def __init__(
        self,
        host: str,
        token: str,
        warehouse_id: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        wait_timeout: str = "40s",
        poll_interval: float = 2.0,
        max_poll_attempts: Optional[int] = 120,
        request_timeout: int = 60,
    ):
        """
        Connector leveraging the Databricks SQL Statement Execution REST API.

        Parameters mirror the payload used in azure/dbx_middleware so existing deployments
        can reuse environment configuration.
        """
        self.type = "DATABRICKS"
        if not token:
            raise ValueError("A Databricks personal access token is required.")
        if not warehouse_id:
            raise ValueError("A Databricks SQL warehouse ID is required.")
        self.connection_kwargs = {
            "host": host,
            "token": token,
            "warehouse_id": warehouse_id,
            "catalog": catalog,
            "schema": schema,
            "wait_timeout": wait_timeout,
            "poll_interval": poll_interval,
            "max_poll_attempts": max_poll_attempts,
            "request_timeout": request_timeout,
        }

    def get_conn(self) -> DatabricksSQLConnection:
        return DatabricksSQLConnection(**self.connection_kwargs)
