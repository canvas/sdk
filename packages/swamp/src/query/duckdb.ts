import { Database } from "duckdb";
import { DataLocation } from "../core/types";
import { all } from "./duckdb.util";

async function getDb(): Promise<Database> {
  return new Database(":memory:");
}

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
      const primaryKeys = Object.entries(table.columnSchema)
        .filter(([, { isPrimaryKey }]) => {
          return isPrimaryKey === true;
        })
        .map(([columnName]) => columnName);
      return `
      CREATE SCHEMA IF NOT EXISTS ${schemaName};
      CREATE OR REPLACE VIEW ${tableSchema} AS 
      SELECT * FROM read_parquet('${
        table.location
      }/*.parquet', union_by_name = true)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY ${primaryKeys.join(
        ","
      )} ORDER BY _swamp_updated_at DESC) = 1;`;
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
  const db = await getDb();
  await addDuckDBTables(db, tables);
  return db;
}
