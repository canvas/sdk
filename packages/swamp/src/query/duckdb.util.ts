import {
  TableData,
  ArrowIterable,
  Database,
  Statement,
  Connection,
  DuckDbError,
} from "duckdb";
import { Result, err, ok } from "neverthrow";
import { generateRandomAlphanumeric, getTempDirectory } from "../util";
import {
  ColumnSchema,
  ColumnsSchema,
  DuckDBType,
  LoaderInserts,
} from "../core/types";

export const DUCKDB_KEYWORDS = [
  "all",
  "analyse",
  "analyze",
  "and",
  "any",
  "array",
  "as",
  "asc",
  "asymmetric",
  "both",
  "case",
  "cast",
  "check",
  "collate",
  "column",
  "constraint",
  "create",
  "default",
  "deferrable",
  "desc",
  "describe",
  "distinct",
  "do",
  "else",
  "end",
  "except",
  "false",
  "fetch",
  "for",
  "foreign",
  "from",
  "grant",
  "group",
  "having",
  "in",
  "initially",
  "intersect",
  "into",
  "lateral",
  "leading",
  "limit",
  "not",
  "null",
  "offset",
  "on",
  "only",
  "or",
  "order",
  "pivot",
  "pivot_longer",
  "pivot_wider",
  "placing",
  "primary",
  "qualify",
  "references",
  "returning",
  "select",
  "show",
  "some",
  "summarize",
  "symmetric",
  "table",
  "then",
  "to",
  "trailing",
  "true",
  "union",
  "unique",
  "unpivot",
  "using",
  "variadic",
  "when",
  "where",
  "window",
  "with",
];

export async function run(
  stmt: Statement,
  ...args: any
): Promise<Result<void, string>> {
  return new Promise((resolve) => {
    stmt.run((error: DuckDbError | null) => {
      if (error) {
        resolve(err(error.message));
      } else {
        resolve(ok(undefined));
      }
    }, args);
  });
}

export async function finalize(stmt: Statement): Promise<Result<void, string>> {
  return new Promise((resolve) => {
    stmt.finalize((error: DuckDbError | null) => {
      if (error) {
        resolve(err(error.message));
      } else {
        resolve(ok(undefined));
      }
    });
  });
}

export async function exec(
  db: Database | Connection,
  query: string,
  args?: any[]
): Promise<Result<void, string>> {
  return new Promise((resolve) => {
    db.exec(query, ...(args || []), (error: DuckDbError | null) => {
      if (error) {
        resolve(err(error.message));
      } else {
        resolve(ok(undefined));
      }
    });
  });
}

export async function all(
  db: Database | Connection,
  query: string,
  args?: any[]
): Promise<Result<TableData, string>> {
  return new Promise((resolve) => {
    db.all(
      query,
      ...(args || []),
      (error: DuckDbError | null, data: TableData) => {
        if (error) {
          resolve(err(error.message));
        } else {
          resolve(ok(data));
        }
      }
    );
  });
}

export async function prepare(
  db: Database,
  query: string
): Promise<Result<Statement, string>> {
  return new Promise((resolve) => {
    db.prepare(query, (error: DuckDbError | null, stmt: Statement) => {
      if (error) {
        resolve(err(error.message));
      } else {
        resolve(ok(stmt));
      }
    });
  });
}

export async function statementAll(
  stmt: Statement
): Promise<Result<TableData, string>> {
  return new Promise((resolve) => {
    stmt.all((error: DuckDbError | null, data: TableData) => {
      if (error) {
        resolve(err(error.message));
      } else {
        resolve(ok(data));
      }
    });
  });
}

export function escapeColumn(column: string): string {
  return DUCKDB_KEYWORDS.includes(column.toLowerCase())
    ? `"${column}"`
    : column;
}

export function getColumnType(columnType: DuckDBType): string {
  return columnType.type === "ARRAY"
    ? `${columnType.innerType}[]`
    : columnType.type;
}

export function generateCreateTableStatement(
  tableName: string,
  schema: Record<string, ColumnSchema>
): string {
  const columns = Object.entries(schema).map(([_columnName, columnSchema]) => {
    const columnType = getColumnType(columnSchema.columnType);
    const isPrimaryKey = columnSchema.isPrimaryKey ? "PRIMARY KEY" : "";
    const columnName = escapeColumn(_columnName);
    return `${columnName} ${columnType} ${isPrimaryKey}`.trim();
  });

  // const columnsDefinition =
  //   columns.join(", ") + ", _swamp_updated_at TIMESTAMP";
  const columnsDefinition = columns.join(", ");
  return `CREATE TABLE ${tableName} (${columnsDefinition})`;
}

type BatchRow = { columns: string[]; values: string[] };
type CreateBatchTableResult = {
  inserts: BatchRow[];
};

function createBatchTable(
  db: Database,
  records: Record<string, any>[],
  schema: Record<string, ColumnSchema>
): CreateBatchTableResult {
  const inserts: BatchRow[] = [];

  for (const record of records) {
    const columns: string[] = [];
    const values: string[] = [];
    for (const [column, value] of Object.entries(record)) {
      if (value !== null) {
        columns.push(escapeColumn(column));
        values.push(value);
      }
    }
    inserts.push({ columns, values });
  }

  return {
    inserts,
  };
}

export type StagedTable = {
  schemaName: string;
  tableName: string;
  inserts: BatchRow[];
  columnSchema: ColumnsSchema;
};

export async function stageInserts(
  db: Database,
  inserts: LoaderInserts
): Promise<StagedTable[]> {
  const queuedWrites: StagedTable[] = [];
  for (const [tableName, batch] of Object.entries(inserts)) {
    const { columnSchema, records, schemaName } = batch;
    if (records.length === 0) {
      continue;
    }

    const { inserts } = createBatchTable(db, records, columnSchema);

    const queuedWrite: StagedTable = {
      inserts,
      columnSchema,
      schemaName,
      tableName,
    };
    queuedWrites.push(queuedWrite);
  }
  return queuedWrites;
}
