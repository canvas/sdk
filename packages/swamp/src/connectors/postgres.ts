import { Client } from "pg";
import { z } from "zod";
import { Inserts, Loader, LoaderResponse } from "../core/types";
import { err, ok, Result } from "neverthrow";
import { sshProxy } from "./postgres/proxy";
import { ConnectionOptions } from "tls";

const FETCH_LIMIT = 5000;

export const Cursor = z.object({
  currentTable: z.string(),
  xminMap: z.record(z.string(), z.number().optional()).optional(),
});
export type Cursor = z.infer<typeof Cursor>;

const TunnelSecrets = z.object({
  host: z.string(),
  port: z.number(),
  user: z.string(),
  privateKey: z.string(),
});

export const Secrets = z.object({
  host: z.string(),
  port: z.number(),
  database: z.string(),
  user: z.string(),
  password: z.string(),
  sshTunnel: TunnelSecrets.optional(),
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

const getPrimaryKeys = async (
  client: Client,
  tableName: string
): Promise<Result<string[], string>> => {
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
  return ok(res.rows.map((row) => row.column_name));
};

const getTableData = async (
  client: Client,
  tableName: string,
  xmin?: number
): Promise<Record<string, any>[]> => {
  try {
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

  const primaryKey = await getPrimaryKeys(client, tableName);
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

async function getClient(
  secrets: Secrets,
  config?: PostgresConfig
): Promise<Client> {
  const { host, port, database, user, password, sshTunnel } = secrets;
  if (sshTunnel) {
    const [proxyHost, proxyPort] = await sshProxy(
      host,
      port,
      sshTunnel.host,
      sshTunnel.port,
      sshTunnel.user,
      sshTunnel.privateKey,
      (error) => {
        console.error(`Error setting up proxy connection: ${error}`);
      }
    );
    return new Client({
      host: proxyHost,
      port: proxyPort,
      database,
      user,
      password,
    });
  }
  if (config?.ssl) {
    return new Client({
      host,
      port,
      database,
      user,
      password,
      ssl: config.ssl,
    });
  }
  return new Client({
    host,
    port,
    database,
    user,
    password,
  });
}

export type PostgresConfig = {
  ssl?: boolean | ConnectionOptions;
  includeTables?: string[];
  initialXmin?: Record<string, string>;
};

export function runPostgresWithConfig(config: PostgresConfig) {
  const generatedFunction = (
    secrets: Secrets | null,
    cursor: Cursor | null
  ) => {
    return runPostgres(secrets, cursor, config);
  };

  generatedFunction.cursor = Cursor;
  generatedFunction.secrets = Secrets;
  generatedFunction satisfies Loader<Secrets, Cursor>;

  return generatedFunction as typeof generatedFunction &
    Loader<Secrets, Cursor>;
}

export async function runPostgres(
  secrets: Secrets | null,
  cursor: Cursor | null,
  config?: PostgresConfig
): Promise<LoaderResponse<Cursor>> {
  if (!secrets) throw new Error("Secrets are required");
  const client = await getClient(secrets, config);
  await client.connect();

  try {
    const _tableNames = await getTableNames(client);
    const tableNames = config?.includeTables
      ? _tableNames.filter((table) => config.includeTables?.includes(table))
      : _tableNames;
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
