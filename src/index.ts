import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { Database } from "sqlite3";
import { promisify } from "util";
import { z } from "zod";

const USER_AGENT = "dj/1.0";

// Create server instance
const server = new McpServer({
    name: "dj",
    version: "1.0.0",
    capabilities: {
        resources: {},
        tools: {},
    },
});

const getDb = () => {
    const db = new Database("database.db");
    return {
        all: promisify<string, any[]>(db.all.bind(db)),
        close: promisify(db.close.bind(db)),
    };
};

server.resource("schema", "schema://main", async (uri) => {
    const db = getDb();
    try {
        const tables = await db.all(
            "SELECT sql FROM sqlite_master WHERE type='table'"
        );
        return {
            contents: [
                {
                    uri: uri.href,
                    text: tables.map((t: { sql: string }) => t.sql).join("\n"),
                },
            ],
        };
    } finally {
        await db.close();
    }
});

server.tool("query", { sql: z.string() }, async ({ sql }) => {
    const db = getDb();
    try {
        const results = await db.all(sql);
        return {
            content: [
                {
                    type: "text",
                    text: JSON.stringify(results, null, 2),
                },
            ],
        };
    } catch (err: unknown) {
        const error = err as Error;
        return {
            content: [
                {
                    type: "text",
                    text: `Error: ${error.message}`,
                },
            ],
            isError: true,
        };
    } finally {
        await db.close();
    }
});

async function main() {
    const transport = new StdioServerTransport();
    await server.connect(transport);
    console.error("DJ MCP Server running on stdio");
}

main().catch((error) => {
    console.error("Fatal error in main():", error);
    process.exit(1);
});
