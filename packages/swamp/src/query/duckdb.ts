import { Database } from "duckdb";
import { DataLocation } from "../core/types";
import { all, generateCreateTableStatement } from "./duckdb.util";

export function sanitizeDuckDb(reference: string): string {
  return reference.replaceAll("-", "_");
}
export function sanitizeTableSchema(table: {
  schemaName: string;
  tableName: string;
}): string {
  return `${sanitizeDuckDb(table.schemaName)}.${sanitizeDuckDb(
    table.tableName
  )}`;
}

export async function addDuckDBTables(
  db: Database,
  tables: DataLocation[]
): Promise<Database> {
  if (tables.length === 0) {
    return db;
  }
  const createTableStatements = tables
    .map((table) => {
      const tableSchema = sanitizeTableSchema(table);
      const schemaName = sanitizeDuckDb(table.schemaName);
      const createTableStatement = generateCreateTableStatement(
        tableSchema,
        table.columnSchema
      );
      return `
      CREATE SCHEMA IF NOT EXISTS ${schemaName};
      ${createTableStatement};
      INSERT INTO ${tableSchema}
        SELECT * FROM read_parquet('${table.location}');`;
    })
    .join(";");
  const result = await all(db, createTableStatements);
  if (result.isErr()) {
    console.log("Error adding table", result.error);
  }
  return db;
}

export async function initializeWithTables(
  tables: DataLocation[]
): Promise<Database> {
  console.log("initializeWithTables", tables);
  const db = new Database(":memory:");
  await addDuckDBTables(db, tables);
  return db;
}
