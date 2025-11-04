# sqltoolkit MCP Server

This project hosts a [FastMCP](https://github.com/openai/fastmcp) server that
wraps the `sqltoolkit` library so MCP-compatible agents can explore and query
databases using the same connectors as the toolkit.

## Configuration

Set the `SQL_CONNECTOR_CONFIG` environment variable to a JSON payload describing
which connector to load. The `type` field selects the connector and the
remaining keys are forwarded to its constructor.

```json
{
  "type": "DATABRICKS",
  "host": "https://adb-<workspace>.azuredatabricks.net",
  "token": "<pat>",
  "warehouse_id": "<sql-warehouse-id>",
  "catalog": "main",
  "schema": "samples"
}
```

Supported connector types:

- `AZURE_SQL`
- `POSTGRESQL`
- `ODBC`
- `SNOWFLAKE`
- `DATABRICKS`

## Running the Server

```bash
cd sqltoolkit_mcp
pip install -r requirements.txt
pip install -e ..
python server.py
```

The server listens on `http://127.0.0.1:9000/mcp` by default. Use the sample
`client.py` to test the MCP interface.

## Available Tools

| Tool | Description |
| ---- | ----------- |
| `list_tables` | Returns tables visible to the configured connector. |
| `table_schema` | Fetches column metadata for a table. |
| `query_sql` | Executes arbitrary read-only SQL. Results are truncated to the optional `limit`. |
| `column_values` | Retrieves distinct values for a column to help craft filters. |
