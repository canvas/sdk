import { ColumnInfo, TableData, Database } from "duckdb";
import { TableStoreInterface } from "../store/sqlite.store";
import {
  createS3SecretStatement,
  generateRandomAlphanumeric,
  sleep,
  uploadFileToS3,
} from "../util";
import {
  ColumnSchema,
  ColumnsSchema,
  CreateRecordsEvent,
  DataLocation,
  LoaderInserts,
  RawLoaderInserts,
  S3Credentials,
} from "../core/types";
import fs from "fs";
import { addDuckDBTables } from "./duckdb";
import { all, DUCKDB_KEYWORDS, prepare, statementAll } from "./duckdb.util";
import { InMemoryLock } from "../lock";
import { err, ok, Result } from "neverthrow";
import { MessageBroker } from "../messages/message.broker";
import { serialize, deserialize } from "v8";

type QueuedWrite = {
  folderLocation: string;
  dataType: "parquet";
  tableName: string;
  schemaName: string;
  columnSchema: ColumnsSchema;
  queuedFilePath: string;
};

export interface SqlEngine {
  querySql(query: string): Promise<QueryResult>;
  writeRecords(inserts: RawLoaderInserts): Promise<void>;
  initialize(messageBroker: MessageBroker): Promise<void>;
}

type DuckDbCredentials = S3Credentials & { type: "s3" };

function escapeColumn(column: string): string {
  return DUCKDB_KEYWORDS.includes(column.toLowerCase())
    ? `"${column}"`
    : column;
}

function generateCreateTableStatement(
  tableName: string,
  schema: Record<string, ColumnSchema>
): string {
  const columns = Object.entries(schema).map(([_columnName, columnSchema]) => {
    const innerColumnType = columnSchema.columnType;
    const columnType =
      innerColumnType.type === "ARRAY"
        ? `${innerColumnType.innerType}[]`
        : innerColumnType.type;
    const isPrimaryKey = columnSchema.isPrimaryKey ? "PRIMARY KEY" : "";
    const columnName = escapeColumn(_columnName);
    return `${columnName} ${columnType} ${isPrimaryKey}`.trim();
  });

  const columnsDefinition =
    columns.join(", ") + ", _swamp_updated_at TIMESTAMP";
  return `CREATE TABLE ${tableName} (${columnsDefinition})`;
}

type CreateBatchTableResult = {
  filePath: string;
  recordCount: number;
};

async function createBatchTable(
  db: Database,
  records: Record<string, any>[],
  schema: Record<string, ColumnSchema>,
  writePath: string
): Promise<Result<CreateBatchTableResult, string>> {
  const tempTable = `temp_table_${generateRandomAlphanumeric(4)}`;
  const createTableStatement = generateCreateTableStatement(tempTable, schema);

  const tryCreateTable = await all(db, createTableStatement);
  if (tryCreateTable.isErr()) {
    console.error(`Error creating table: ${tryCreateTable.error}`);
    return err(tryCreateTable.error);
  }

  for (const record of records) {
    const columns = [],
      values = [];
    for (const [column, value] of Object.entries(record)) {
      if (value !== null) {
        columns.push(escapeColumn(column));
        values.push(value);
      }
    }
    columns.push("_swamp_updated_at");
    values.push(new Date());
    try {
      const tryInsert = await all(
        db,
        `insert into ${tempTable} (${columns.join(", ")}) values (${values
          .map(() => "?")
          .join(", ")})`,
        values
      );
      if (tryInsert.isErr()) {
        console.error("Insert error", tryInsert.error);
        console.log("Create table statement", createTableStatement);
        return err(tryInsert.error);
      }
    } catch (e) {
      console.error(e);
    }
  }

  const tryCopy = `
      copy ${tempTable} to '${writePath}' (format parquet);
      drop table ${tempTable};
    `;

  const result = await all(db, tryCopy);
  if (result.isErr()) {
    console.error(result.error);
    return err(result.error);
  }
  return ok({
    filePath: writePath,
    recordCount: records.length,
  });
}

type CreateRecords = { buffer: Buffer };

export class InMemoryDuckDb implements SqlEngine {
  db: Database | null;

  credentials: DuckDbCredentials | null;

  writeLock: InMemoryLock;

  writePath: string;

  store: TableStoreInterface;

  newRecordsQueue: CreateRecords[] = [];

  uploadQueue: QueuedWrite[] = [];

  constructor(store: TableStoreInterface) {
    this.db = null;
    this.credentials = null;
    this.writeLock = new InMemoryLock();
    this.writePath = "data";
    this.store = store;
    this.runUploadLoop();
  }

  async initialize(messageBroker: MessageBroker): Promise<void> {
    const tables = await this.store.getTables();
    await addDuckDBTables(await this.getDb(), tables);
    messageBroker.subscribeToNewRecords(this.handleCreateRecords.bind(this));
  }

  async handleCreateRecords(event: CreateRecordsEvent): Promise<void> {
    const buffer = serialize(event);
    this.newRecordsQueue.push({ buffer });
  }

  dequeueNewRecords(): CreateRecords | null {
    if (this.newRecordsQueue.length === 0) {
      return null;
    }
    return this.newRecordsQueue.shift()!;
  }

  async runUploadLoop() {
    while (true) {
      const newRecords = this.dequeueNewRecords();
      if (newRecords) {
        const inserts: CreateRecordsEvent = deserialize(newRecords.buffer);
        await this.writeRecords(inserts.inserts);
      }
      const queuedWrite = this.dequeueWrite();
      if (!queuedWrite) {
        await sleep(1000);
        continue;
      }
      await this.commitTableToStorage(queuedWrite);
    }
  }

  queueWrite(write: QueuedWrite) {
    this.uploadQueue.push(write);
  }

  dequeueWrite(): QueuedWrite | null {
    if (this.uploadQueue.length === 0) {
      return null;
    }
    return this.uploadQueue.shift()!;
  }

  async commitTableToStorage(
    write: QueuedWrite
  ): Promise<Result<void, string>> {
    const {
      folderLocation,
      tableName,
      schemaName,
      columnSchema,
      queuedFilePath,
    } = write;

    let dataLocation: DataLocation = {
      location: folderLocation,
      tableName,
      schemaName,
      columnSchema,
      dataType: "parquet" as const,
      fileLocation: "local",
    };

    if (this.credentials) {
      const tryUpload = await uploadFileToS3(this.credentials, queuedFilePath);
      fs.unlinkSync(queuedFilePath);
      if (tryUpload.isErr()) {
        console.error(`Error uploading to S3: ${tryUpload.error}`);
        return err(tryUpload.error);
      }
      dataLocation.fileLocation = "s3";
      dataLocation.location = `s3://${this.credentials.bucket}/${folderLocation}`;
    }
    const existingTable = await this.store.getTable(schemaName, tableName);
    if (existingTable === null) {
      await this.addNewTable(dataLocation);
    } else {
      await this.updateTables([dataLocation]);
    }
    return ok(undefined);
  }

  getFileName(): string {
    return `${new Date().getTime()}-${generateRandomAlphanumeric(4)}.parquet`;
  }

  getLocalWriteFolder(schemaName: string, tableName: string): string {
    const schemaFilePath = `${this.writePath}/${schemaName}`;
    return `${schemaFilePath}/${tableName}`;
  }

  async writeRecords(inserts: LoaderInserts): Promise<void> {
    await this.writeLock.acquire();
    try {
      const db = await this.getDb();

      for (const [tableName, batch] of Object.entries(inserts)) {
        const { columnSchema, records, schemaName } = batch;
        if (records.length === 0) {
          console.log("no records");
          continue;
        }

        const writeFolder = this.getLocalWriteFolder(schemaName, tableName);
        fs.mkdirSync(writeFolder, { recursive: true });

        const fileName = this.getFileName();
        const writePath = `${writeFolder}/${fileName}`;

        const parquetFile = await createBatchTable(
          db,
          records,
          columnSchema,
          writePath
        );
        if (parquetFile.isErr()) {
          console.error(
            `Error writing table ${schemaName}.${tableName}: ${parquetFile.error}`
          );
          continue;
        }
        console.log(
          `Wrote ${parquetFile.value.recordCount} records to table ${schemaName}.${tableName}`
        );

        const queuedWrite: QueuedWrite = {
          folderLocation: writeFolder,
          dataType: "parquet" as const,
          tableName,
          schemaName,
          columnSchema,
          queuedFilePath: parquetFile.value.filePath,
        };

        this.queueWrite(queuedWrite);
      }
    } finally {
      this.writeLock.release();
    }
  }

  withS3Credentials(credentials: S3Credentials): InMemoryDuckDb {
    this.credentials = { ...credentials, type: "s3" };
    return this;
  }

  async addNewTable(table: DataLocation): Promise<void> {
    await this.store.addTable(table);
    await this.updateTables([table]);
  }

  async updateTables(tables: DataLocation[]): Promise<void> {
    const db = await this.getDb();
    await addDuckDBTables(db, tables);
  }

  async initDb(): Promise<Database> {
    const db = new Database(":memory:");
    if (this.credentials) {
      if (this.credentials.type === "s3") {
        await all(db, createS3SecretStatement(this.credentials));
      }
    }
    await all(db, "INSTALL arrow; LOAD arrow;");
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
