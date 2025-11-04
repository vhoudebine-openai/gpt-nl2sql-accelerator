# gpt-nl2sql-accelerator

An accelerator for building natural-language-to-SQL agents powered by OpenAI
products—whether you are integrating through the ChatGPT UI, the Agents SDK, Responses API. The project packages everything needed to harvest
database context, reason about schemas, and safely execute read-only SQL across
multiple providers.

## Key Components

- **sqltoolkit** – Python library that connects to Azure SQL, PostgreSQL,
  Snowflake, Databricks (via REST), and generic ODBC sources. It extracts table
  metadata, samples rows, generates natural-language descriptions, and runs
  queries through a common `DatabaseClient`.
- **sqltoolkit_middleware** – Azure Functions app that exposes sqltoolkit
  operations over HTTP for ChatGPT or other web-centric agents. Responses are
  returned as JSON for small result sets and streamed as downloadable CSV files
  whenever a query returns more than 10 rows.
- **sqltoolkit_mcp** – A FastMCP server that reuses sqltoolkit connectors so MCP
  hosts (such as OpenAI’s desktop client) can list tables, inspect schemas, run
  queries, and fetch distinct column values.
- **notebooks** – Quickstart notebooks demonstrating how to index new databases
  and experiment with generated SQL.

## Database Indexing Workflow

1. **Connect** – Instantiate the appropriate connector (AzureSQLConnector,
   PostgreSQLConnector, SnowflakeConnector, DatabricksConnector, or OdbcConnector)
   and wrap it with `DatabaseClient`.
2. **Profile tables** – Use `DatabaseIndexer` to enumerate tables, capture column
   metadata, sample key values, and capture row-level examples.
3. **Enrich with language** – Call the built-in prompt templates to create
   human-friendly descriptions that improve grounding for downstream LLM calls.
4. **Embed and store** – Generate embeddings (e.g., `text-embedding-3-small`) and
   push the table manifests to your vector store. The included helpers target
   Azure AI Search, but the exported manifest can seed any retrieval system.
5. **Serve at query time** – When the agent receives a natural-language question,
   retrieve the top table manifests, assemble a SQL-focused prompt, and use
   sqltoolkit (or the middleware/MCP layers) to validate and execute the SQL.

This separation keeps heavy metadata collection offline while the online path
stays fast and focused on query execution.

## Middleware Options

- **ChatGPT / Web Agents** – Deploy `sqltoolkit_middleware` to Azure Functions,
  configure the `SQL_CONNECTOR_CONFIG` environment variable with your connector
  settings, and call the `/api/sql/query` endpoint. JSON results are returned
  when <= 10 rows; otherwise the middleware streams a CSV attachment with an
  `X-Row-Count` header so ChatGPT can expose it as a downloadable file.
- **MCP Hosts** – Run `python sqltoolkit_mcp/server.py` (after installing
  `sqltoolkit_mcp/requirements.txt` and `pip install -e .`). MCP clients can
  discover tools such as `list_tables`, `table_schema`, `query_sql`, and
  `column_values`, making the same connectors available inside desktop agents.

## Installation

```bash
pip install -r requirements.txt
```

For component-specific dependencies:

- `pip install -r sqltoolkit_middleware/requirements.txt && pip install -e .`
  to run the Azure Functions middleware locally.
- `pip install -r sqltoolkit_mcp/requirements.txt && pip install -e .` to host
  the MCP server.

## Basic Usage

```python
from sqltoolkit.connectors import AzureSQLConnector, DatabricksConnector
from sqltoolkit.client import DatabaseClient

# Azure SQL via Entra ID
azure_connector = AzureSQLConnector(server="your-server.database.windows.net", database="your_db")
sql_client = DatabaseClient(azure_connector)

# Databricks via REST API
dbx_connector = DatabricksConnector(
    host="https://adb-<workspace>.azuredatabricks.net",
    token="<personal-access-token>",
    warehouse_id="<sql-warehouse-id>",
    catalog="main",
    schema="sales",
)
dbx_client = DatabaseClient(dbx_connector)

print(sql_client.list_database_tables())
print(dbx_client.query("SELECT * FROM sales.orders LIMIT 5"))
```

Use `sqltoolkit.indexer.DatabaseIndexer` to collect and embed metadata, then wire
the middleware or MCP server into your preferred agent runtime to close the loop
from natural language to validated SQL and back.
