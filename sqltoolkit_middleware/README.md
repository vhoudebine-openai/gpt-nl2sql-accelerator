# sqltoolkit Azure Function Middleware

This Azure Functions app exposes the `sqltoolkit` library over simple HTTP
endpoints so that downstream services can execute SQL, inspect schemas, and
download large result sets.

## Environment Configuration

Set `SQL_CONNECTOR_CONFIG` to a JSON blob that describes which connector the
middleware should load. The `type` field selects the connector and the
remaining keys are passed directly to the constructor.

```json
{
  "type": "DATABRICKS",
  "host": "https://adb-<workspace>.azuredatabricks.net",
  "token": "<personal-access-token>",
  "warehouse_id": "<sql-warehouse-id>",
  "catalog": "main",
  "schema": "sales"
}
```

Supported `type` values: `AZURE_SQL`, `POSTGRESQL`, `ODBC`, `SNOWFLAKE`,
`DATABRICKS`.

When running locally copy `local.settings.sample.json` to `local.settings.json`
and update the placeholder values. Install dependencies with:

```bash
cd middleware/sqltoolkit_middleware
pip install -r requirements.txt
pip install -e ../..
```

## Endpoints

| Method | Route | Description |
| ------ | ----- | ----------- |
| `GET` | `/api/sql/tables` | Lists tables using `DatabaseClient.list_database_tables`. |
| `GET` | `/api/sql/schema/{table_name}` | Fetches schema metadata for the provided table. |
| `POST` | `/api/sql/query` | Executes the supplied SQL statement. Returns JSON when the result contains 10 or fewer rows; otherwise streams a CSV download. |

For large result sets (`> 10` rows) the response includes
`Content-Disposition: attachment` and an `X-Row-Count` header. Provide an
optional `filename` property in the request body to control the CSV file name.
