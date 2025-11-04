import asyncio

from fastmcp import Client


# Replace the URL with the transport exposed by your sqltoolkit MCP server.
client = Client("http://127.0.0.1:9000/mcp")


async def main():
    async with client:
        await client.ping()

        tools = await client.list_tools()
        print("Available tools:", tools)

        tables = await client.call_tool("list_tables", {})
        print("Tables:", tables)

        # Execute a sample query (adjust for your catalog/schema).
        result = await client.call_tool(
            "query_sql",
            {"sql": "SELECT * FROM your_schema.your_table LIMIT 5;", "limit": 20},
        )
        print("Query result:", result)


asyncio.run(main())
