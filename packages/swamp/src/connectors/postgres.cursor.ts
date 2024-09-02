import { Client } from "pg";
import { z } from "zod";
import { Inserts, Loader, LoaderResponse } from "../core/types";
import { err, ok, Result } from "neverthrow";
import { PostgresBase, Secrets, PostgresConfig } from "./postgres.base";

const FETCH_LIMIT = 1000;

export const Cursor = z.object({
  currentTable: z.string(),
  cursorPositionMap: z
    .record(z.string(), z.union([z.string(), z.number()]).optional())
    .optional(),
});
export type Cursor = z.infer<typeof Cursor>;

type CursorColumn = {
  cursorColumnName: string;
  tableName: string;
  initialValue?: string;
};

type CursorPosition = {
  cursorColumn: string;
  currentCursorValue?: string | number;
};

export type PostgresCursorConfig = PostgresConfig & {
  cursorColumns: CursorColumn[];
};

export class PostgresCursor extends PostgresBase<typeof Cursor> {
  constructor(private cursorConfig: PostgresCursorConfig) {
    super(cursorConfig);
  }

  getCursor() {
    return Cursor;
  }

  protected async getTableData(
    client: Client,
    tableName: string,
    cursorPosition: CursorPosition
  ): Promise<Record<string, any>[]> {
    try {
      this.setupParsers(client);

      const { cursorColumn, currentCursorValue } = cursorPosition;

      const query = currentCursorValue
        ? `SELECT * FROM public.${tableName} WHERE ${cursorColumn} >= $1 order by ${cursorColumn} limit ${FETCH_LIMIT}`
        : `SELECT * FROM public.${tableName} order by ${cursorColumn} limit ${FETCH_LIMIT}`;

      const res = await client.query({
        text: query,
        values: currentCursorValue ? [currentCursorValue] : [],
      });
      return res.rows;
    } catch (error) {
      console.error(`Error fetching data from ${tableName}:`, error);
      throw error;
    }
  }

  private getCurrentTablePosition(cursor: Cursor | null): {
    tableName: string;
    nextTableName: string;
    hasMoreTables: boolean;
  } {
    const tableNames = this.cursorConfig.cursorColumns.map(
      (column) => column.tableName
    );
    const currentTableIndex = cursor
      ? tableNames.indexOf(cursor.currentTable)
      : -1;
    const tableName = cursor?.currentTable || tableNames[0];

    const nextTableIndex = currentTableIndex + 1;
    const hasMoreTables = nextTableIndex < tableNames.length;
    const nextTableName = hasMoreTables
      ? tableNames[nextTableIndex]
      : tableNames[0];
    return {
      tableName,
      nextTableName,
      hasMoreTables,
    };
  }

  private getCursorPosition(
    cursor: Cursor | null,
    tableName: string
  ): Result<CursorPosition, string> {
    const currentCursorValue = cursor?.cursorPositionMap?.[tableName];
    const cursorColumn = this.cursorConfig.cursorColumns.find(
      (column) => column.tableName === tableName
    );
    const cursorColumnName = cursorColumn?.cursorColumnName;
    const cursorColumnInitialValue = cursorColumn?.initialValue;
    if (!cursorColumnName) {
      return err(`Cursor column not found for table ${tableName}`);
    }
    return ok({
      cursorColumn: cursorColumnName,
      currentCursorValue: currentCursorValue || cursorColumnInitialValue,
    });
  }

  protected async getRecords(
    cursor: Cursor | null,
    client: Client
  ): Promise<{ cursor: Cursor; inserts: Inserts; hasMore: boolean }> {
    const { tableName, nextTableName, hasMoreTables } =
      this.getCurrentTablePosition(cursor);

    const primaryKey = await this.getPrimaryKey(client, tableName);
    if (primaryKey.isErr()) {
      console.warn(primaryKey.error);
      return {
        cursor: {
          currentTable: nextTableName,
          cursorPositionMap: cursor?.cursorPositionMap,
        },
        inserts: {},
        hasMore: hasMoreTables,
      };
    }

    const currentCursorPosition = this.getCursorPosition(cursor, tableName);
    if (currentCursorPosition.isErr()) {
      console.warn(currentCursorPosition.error);
      return {
        cursor: {
          currentTable: nextTableName,
          cursorPositionMap: cursor?.cursorPositionMap,
        },
        inserts: {},
        hasMore: hasMoreTables,
      };
    }
    const records = await this.getTableData(
      client,
      tableName,
      currentCursorPosition.value
    );
    console.log(`Got ${records.length} records for table ${tableName}`);
    const inserts: Inserts = {
      [tableName]: { primaryKeys: primaryKey.value, records },
    };

    const newCursorValue =
      records.length > 0
        ? records[records.length - 1][currentCursorPosition.value.cursorColumn]
        : currentCursorPosition.value.currentCursorValue;

    const cursorPositionMap = {
      ...cursor?.cursorPositionMap,
      [tableName]: newCursorValue,
    };

    if (records.length === FETCH_LIMIT) {
      return {
        cursor: { currentTable: tableName, cursorPositionMap },
        inserts,
        hasMore: true,
      };
    }

    if (!hasMoreTables) {
      return {
        cursor: { currentTable: nextTableName, cursorPositionMap },
        inserts,
        hasMore: false,
      };
    }
    return {
      cursor: { currentTable: nextTableName, cursorPositionMap },
      inserts,
      hasMore: true,
    };
  }

  public async run(
    secrets: Secrets | null,
    cursor: Cursor | null
  ): Promise<LoaderResponse<Cursor>> {
    if (!secrets) throw new Error("Secrets are required");
    const client = await this.getClient(secrets);

    try {
      const {
        inserts,
        hasMore,
        cursor: newCursor,
      } = await this.getRecords(cursor, client);
      return {
        type: "success",
        inserts,
        cursor: newCursor,
        hasMore,
      };
    } finally {
      if (!this.client) {
        await client.end();
      }
    }
  }
}

export function runPostgresCursorWithConfig(config: PostgresCursorConfig) {
  const postgresLoader = new PostgresCursor(config);
  const generatedFunction = (
    secrets: Secrets | null,
    cursor: Cursor | null
  ) => {
    return postgresLoader.run(secrets, cursor);
  };

  generatedFunction.cursor = Cursor;
  generatedFunction.secrets = Secrets;
  generatedFunction satisfies Loader<Secrets, Cursor>;

  return generatedFunction as typeof generatedFunction &
    Loader<Secrets, Cursor>;
}
