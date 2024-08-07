import { Client } from "pg";
import { z } from "zod";
import { Inserts, Loader, LoaderResponse } from "../core/types";
import { err, ok, Result } from "neverthrow";

const FETCH_LIMIT = 1000;

export const Cursor = z.object({
  currentTable: z.string(),
  xminMap: z.record(z.string(), z.number().optional()).optional(),
});
export type Cursor = z.infer<typeof Cursor>;

export const Secrets = z.object({
  host: z.string(),
  port: z.number(),
  database: z.string(),
  user: z.string(),
  password: z.string(),
});
export type Secrets = z.infer<typeof Secrets>;

const getTableNames = async (client: Client): Promise<string[]> => {
  try {
    const res = await client.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
      ORDER BY table_name
    `);
    return res.rows.map((row) => row.table_name);
  } catch (error) {
    console.error("Error fetching table names:", error);
    throw error;
  }
};

const getPrimaryKey = async (
  client: Client,
  tableName: string
): Promise<Result<string[], string>> => {
  try {
    const res = await client.query(
      `
	SELECT a.attname AS column_name
	FROM pg_index i
	JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
	WHERE i.indrelid = $1::regclass AND i.indisprimary
	`,
      [tableName]
    );
    if (res.rows.length === 0) {
      return err(`No primary key found for table ${tableName}`);
    }
    if (res.rows.length > 1) {
      console.warn(`Multiple primary keys found for table ${tableName}`);
      return err(`Multiple primary keys found for table ${tableName}`);
    }
    return ok(res.rows.map((row) => row.column_name));
  } catch (error) {
    console.error(`Error fetching primary key for table ${tableName}:`, error);
    throw error;
  }
};

const getTableData = async (
  client: Client,
  tableName: string,
  xmin?: number
): Promise<Record<string, any>[]> => {
  try {
    const jsonParser = client.getTypeParser(114); // OID 114 is for JSON
    const jsonbParser = client.getTypeParser(3802); // OID 3802 is for JSONB
    const arrayParser = client.getTypeParser(1007); // OID 1007 is for integer arrays, adjust as necessary

    client.setTypeParser(114, (value: string) =>
      JSON.stringify(jsonParser(value))
    );
    client.setTypeParser(3802, (value: string) =>
      JSON.stringify(jsonbParser(value))
    );
    client.setTypeParser(1007, (value: string) =>
      JSON.stringify(arrayParser(value))
    );

    const query = xmin
      ? `SELECT *, xmin::text::bigint FROM public.${tableName} WHERE xmin::text::bigint > $1 order by xmin::text::bigint limit ${FETCH_LIMIT}`
      : `SELECT *, xmin::text::bigint FROM public.${tableName} order by xmin::text::bigint limit ${FETCH_LIMIT}`;

    const res = await client.query({
      text: query,
      values: xmin ? [xmin] : [],
    });
    return res.rows;
  } catch (error) {
    console.error(`Error fetching data from ${tableName}:`, error);
    throw error;
  }
};

async function getRecords(
  cursor: Cursor | null,
  client: Client,
  tableNames: string[]
): Promise<{ cursor: Cursor; inserts: Inserts; hasMore: boolean }> {
  const currentTableIndex = cursor
    ? tableNames.indexOf(cursor.currentTable)
    : -1;
  const tableName = cursor?.currentTable || tableNames[0];

  const nextTableIndex = currentTableIndex + 1;
  const hasMoreTables = nextTableIndex < tableNames.length - 1;
  const nextTableName = hasMoreTables
    ? tableNames[nextTableIndex]
    : tableNames[0];

  const primaryKey = await getPrimaryKey(client, tableName);
  if (primaryKey.isErr()) {
    console.warn(primaryKey.error);
    return {
      cursor: { currentTable: nextTableName, xminMap: cursor?.xminMap },
      inserts: {},
      hasMore: hasMoreTables,
    };
  }
  const previousXmin = cursor?.xminMap?.[tableName];
  const records = await getTableData(client, tableName, previousXmin);
  console.log(`Got ${records.length} records for table ${tableName}`);
  const filteredRecords = records.map(({ xmin, ...rest }) => rest);
  const inserts: Inserts = {
    [tableName]: { primaryKeys: primaryKey.value, records: filteredRecords },
  };

  const newXmin =
    records.length > 0
      ? Math.max(...records.map((record) => record.xmin))
      : previousXmin;

  const newXminMap = { ...cursor?.xminMap, [tableName]: newXmin };

  if (records.length === FETCH_LIMIT) {
    return {
      cursor: { currentTable: tableName, xminMap: newXminMap },
      inserts,
      hasMore: true,
    };
  }

  if (!hasMoreTables) {
    return {
      cursor: { currentTable: nextTableName, xminMap: newXminMap },
      inserts,
      hasMore: false,
    };
  }
  return {
    cursor: { currentTable: nextTableName, xminMap: newXminMap },
    inserts,
    hasMore: true,
  };
}

export async function runPostgres(
  secrets: Secrets | null,
  cursor: Cursor | null
): Promise<LoaderResponse<Cursor>> {
  if (!secrets) throw new Error("Secrets are required");
  const { host, port, database, user, password } = secrets;
  const client = new Client({ host, port, database, user, password });
  await client.connect();

  try {
    const tableNames = await getTableNames(client);
    if (tableNames.length === 0)
      throw new Error("No tables found in the database");

    const {
      inserts,
      hasMore,
      cursor: newCursor,
    } = await getRecords(cursor, client, tableNames);
    return {
      type: "success",
      inserts,
      cursor: newCursor,
      hasMore,
    };
  } finally {
    await client.end();
  }
}

runPostgres satisfies Loader<Secrets, Cursor>;
runPostgres.cursor = Cursor;
runPostgres.secrets = Secrets;
