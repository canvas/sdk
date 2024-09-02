import { Client } from "pg";
import { z } from "zod";
import { Inserts, Loader, LoaderResponse } from "../core/types";
import { err, ok, Result } from "neverthrow";
import { sshProxy } from "./postgres/proxy";
import { ConnectionOptions } from "tls";

const FETCH_LIMIT = 1000;

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

export abstract class PostgresBase<TCursor extends z.ZodType> {
  protected client: Client | null = null;

  constructor(protected config: PostgresConfig) {}

  abstract getCursor(): TCursor;

  protected async getClient(secrets: Secrets): Promise<Client> {
    if (this.client) {
      return this.client;
    }
    const newClient = await this._getClient(secrets);
    await newClient.connect();
    this.client = newClient;
    return this.client;
  }

  private async _getClient(secrets: Secrets): Promise<Client> {
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
    if (this.config.ssl) {
      return new Client({
        host,
        port,
        database,
        user,
        password,
        ssl: this.config.ssl,
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

  protected async getPrimaryKey(
    client: Client,
    tableName: string
  ): Promise<Result<string[], string>> {
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
      return err(`Multiple primary keys found for table ${tableName}`);
    }
    return ok(res.rows.map((row) => row.column_name));
  }

  protected setupParsers(client: Client): void {
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
  }

  protected abstract getTableData(
    client: Client,
    tableName: string,
    cursorValue: any
  ): Promise<Record<string, any>[]>;

  protected abstract getRecords(
    cursor: z.infer<TCursor> | null,
    client: Client,
    tableNames: string[]
  ): Promise<{
    cursor: z.infer<TCursor>;
    inserts: Inserts;
    hasMore: boolean;
  }>;

  public abstract run(
    secrets: Secrets | null,
    cursor: z.infer<TCursor> | null
  ): Promise<LoaderResponse<z.infer<TCursor>>>;
}

export type PostgresConfig = {
  ssl?: boolean | ConnectionOptions;
  includeTables?: string[];
};
