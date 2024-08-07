import {
  TableData,
  ArrowIterable,
  Database,
  Statement,
  Connection,
  DuckDbError,
} from "duckdb";
import { Result, err, ok } from "neverthrow";

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
