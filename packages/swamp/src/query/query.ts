import { ColumnInfo, TableData, Database } from "duckdb";
import { TableStoreInterface } from "../store/sqlite.store";
import { createS3SecretStatement } from "../util";
import { DataLocation, S3Credentials } from "../core/types";
import { addDuckDBTables } from "./duckdb";
import { all, prepare, statementAll } from "./duckdb.util";
import { MessageBroker } from "../messages/message.broker";

export interface SqlEngine {
  querySql(query: string): Promise<QueryResult>;
  initialize(
    tables: DataLocation[],
    messageBroker: MessageBroker
  ): Promise<void>;
}

export class InMemoryDuckDb implements SqlEngine {
  db: Database | null = null;

  credentials: S3Credentials | null = null;

  withS3Credentials(credentials: S3Credentials): InMemoryDuckDb {
    this.credentials = credentials;
    return this;
  }

  async initialize(
    tables: DataLocation[],
    messageBroker: MessageBroker
  ): Promise<void> {
    await addDuckDBTables(await this.getDb(), tables);
    messageBroker.subscribeToNewDataLocation(
      this.handleNewDataLocation.bind(this)
    );
  }

  async handleNewDataLocation(dataLocation: DataLocation): Promise<void> {
    await this.updateTables([dataLocation]);
  }

  async updateTables(tables: DataLocation[]): Promise<void> {
    const db = await this.getDb();
    await addDuckDBTables(db, tables);
  }

  async initDb(): Promise<Database> {
    const db = new Database(":memory:");
    if (this.credentials) {
      await all(db, createS3SecretStatement(this.credentials));
    }
    return db;
  }

  async getDb(): Promise<Database> {
    if (!this.db) {
      this.db = await this.initDb();
    }
    return this.db;
  }

  async querySql(query: string): Promise<QueryResult> {
    const db = await this.getDb();
    const statement = await prepare(db, query);
    if (statement.isErr()) {
      throw new Error(statement.error);
    }
    const columns = statement.value.columns();
    const data = await statementAll(statement.value);
    if (data.isErr()) {
      throw new Error(data.error);
    }
    return { data: data.value, columns };
  }
}
export type QueryResult = {
  data: TableData;
  columns: ColumnInfo[];
};
