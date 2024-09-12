import { ColumnInfo, TableData, Database } from "duckdb";
import { TableStoreInterface } from "../store/sqlite.store";
import {
  createS3SecretStatement,
  fetchFileFromS3,
  uploadFileToS3,
} from "../util";
import {
  ColumnSchema,
  CreateRecordsEvent,
  DataLocation,
  DuckDBType,
  S3Credentials,
} from "../core/types";
import { addDuckDBTables } from "./duckdb";
import {
  all,
  exec,
  generateCreateTableStatement,
  stageInserts,
  prepare,
  StagedTable,
  statementAll,
  getColumnType,
} from "./duckdb.util";
import { MessageBroker } from "../messages/message.broker";
import { Result, ok, err } from "neverthrow";
import fs from "fs";
import debounce from "debounce";

export type QueryResult = {
  data: TableData;
  columns: ColumnInfo[];
};

export interface SqlEngine {
  querySql(query: string): Promise<QueryResult>;
  initialize(messageBroker: MessageBroker): Promise<void>;
}

type QueuedWrite = {
  schemaName: string;
  tableName: string;
  columnSchema: Record<string, ColumnSchema>;
};

type SchemaChange = {
  type: "add_column";
  columnName: string;
  columnType: DuckDBType;
};

type MergeResult = {
  schemaChanges: SchemaChange[];
  columnSchema: Record<string, ColumnSchema>;
  missingInsertColumns: string[];
};

function mergeSchemas(
  existingSchema: Record<string, ColumnSchema>,
  newSchema: Record<string, ColumnSchema>
): MergeResult {
  const schemaChanges: SchemaChange[] = [];
  const columnSchema: Record<string, ColumnSchema> = existingSchema;
  const missingInsertColumns: string[] = [];
  for (const [key, value] of Object.entries(newSchema)) {
    if (!existingSchema[key]) {
      schemaChanges.push({
        type: "add_column",
        columnName: key,
        columnType: value.columnType,
      });
      columnSchema[key] = value;
    }
  }
  for (const key of Object.keys(existingSchema)) {
    if (!newSchema[key]) {
      missingInsertColumns.push(key);
    }
  }
  return { schemaChanges: schemaChanges, columnSchema, missingInsertColumns };
}

const DATA_ROOT = "data";

export class DuckDbEngine implements SqlEngine {
  db: Database | null = null;

  credentials: S3Credentials | null = null;

  store: TableStoreInterface;

  private queuedWrites: Map<string, QueuedWrite> = new Map();
  private debouncedWrites: Map<string, ReturnType<typeof debounce>> = new Map();

  constructor(store: TableStoreInterface) {
    this.store = store;
  }

  withS3Credentials(credentials: S3Credentials): DuckDbEngine {
    this.credentials = credentials;
    return this;
  }

  async downloadData(): Promise<DataLocation[]> {
    const tables = await this.store.getTables();
    return Promise.all(
      tables.map(async (table) => {
        if (table.fileLocation === "s3") {
          if (!this.credentials) {
            throw new Error("Table data is on S3 but no credentials provided");
          }

          const dataDir = `${DATA_ROOT}/${table.schemaName}`;
          fs.mkdirSync(dataDir, { recursive: true });

          const fileName = await fetchFileFromS3(
            this.credentials,
            table.location
          );
          if (!fileName) {
            throw new Error("Failed to download file");
          }
          return {
            ...table,
            location: fileName,
          };
        } else {
          return table;
        }
      })
    );
  }

  async initialize(messageBroker: MessageBroker): Promise<void> {
    const localTableLocations = await this.downloadData();
    await addDuckDBTables(await this.getDb(), localTableLocations);
    messageBroker.subscribeToNewRecords(this.handleNewRecords.bind(this));
  }

  async writeTableLocal(queuedWrite: QueuedWrite): Promise<string> {
    const db = await this.getDb();
    const { schemaName, tableName } = queuedWrite;
    const folderLocation = `${DATA_ROOT}/${schemaName}`;
    fs.mkdirSync(folderLocation, { recursive: true });
    // use duckdb to write parquet file
    const parquetFilePath = `${folderLocation}/${tableName}.parquet`;
    const result = await exec(
      db,
      `COPY ${schemaName}.${tableName} TO '${parquetFilePath}' (FORMAT 'parquet')`
    );
    if (result.isErr()) {
      throw new Error(result.error);
    }
    return parquetFilePath;
  }

  async writeTable(
    queuedWrite: QueuedWrite
  ): Promise<Result<DataLocation, string>> {
    const parquetFilePath = await this.writeTableLocal(queuedWrite);
    if (this.credentials) {
      const tryUpload = await uploadFileToS3(
        this.credentials,
        parquetFilePath,
        parquetFilePath
      );
      if (tryUpload.isErr()) {
        console.error(`Error uploading file: ${tryUpload.error}`);
        return err(`Error uploading file: ${tryUpload.error}`);
      } else {
        return ok({
          ...queuedWrite,
          location: tryUpload.value,
          dataType: "parquet",
          fileLocation: "s3",
        });
      }
    } else {
      return ok({
        ...queuedWrite,
        location: parquetFilePath,
        dataType: "parquet",
        fileLocation: "local",
      });
    }
  }

  async getTable(
    schemaName: string,
    tableName: string
  ): Promise<{ columnSchema: Record<string, ColumnSchema> } | null> {
    const existingTable = await this.store.getTable(schemaName, tableName);
    if (existingTable) {
      return existingTable;
    }
    const queuedWrite = this.queuedWrites.get(`${schemaName}.${tableName}`);
    if (queuedWrite) {
      return queuedWrite;
    }
    return null;
  }

  async mergeStagedTable(
    stagedTable: StagedTable
  ): Promise<Result<QueuedWrite, string>> {
    const db = await this.getDb();
    const {
      inserts,
      columnSchema: newColumnSchema,
      schemaName,
      tableName,
    } = stagedTable;
    const fullTableName = `${schemaName}.${tableName}`;

    const existingTable = await this.getTable(schemaName, tableName);

    if (!existingTable) {
      // must create a new table in order to preserve primary keys
      const createTableStatement = generateCreateTableStatement(
        fullTableName,
        newColumnSchema
      );
      const createTableStmt = `CREATE SCHEMA IF NOT EXISTS ${schemaName};
      ${createTableStatement};
      `;
      const renameResult = await all(db, createTableStmt);
      if (renameResult.isErr()) {
        return err(
          `Error creating new table running ${createTableStmt}: ${renameResult.error}`
        );
      }
      for (const batchRow of inserts) {
        const { columns, values } = batchRow;
        const tryInsert = await all(
          db,
          `insert into ${fullTableName} (${columns.join(", ")}) values (${values
            .map(() => "?")
            .join(", ")})`,
          values
        );
        if (tryInsert.isErr()) {
          console.error("Insert error", tryInsert.error);
          return err(tryInsert.error);
        }
      }

      return ok({
        tableName,
        schemaName,
        columnSchema: newColumnSchema,
      });
    }

    const { columnSchema: existingColumnSchema } = existingTable;

    const {
      schemaChanges: actions,
      columnSchema: mergedColumnSchema,
      missingInsertColumns,
    } = mergeSchemas(existingColumnSchema, newColumnSchema);

    const primaryKeys = Object.entries(mergedColumnSchema)
      .filter(([_, schema]) => schema.isPrimaryKey)
      .map(([key, _]) => key);
    if (primaryKeys.length === 0) {
      return err("No primary key found in column schema");
    }

    // apply merge results
    for (const mergeAction of actions) {
      switch (mergeAction.type) {
        case "add_column":
          const columnType = getColumnType(mergeAction.columnType);
          const mergeStmt = `ALTER TABLE ${fullTableName} ADD COLUMN ${mergeAction.columnName} ${columnType}`;
          const addColumnResult = await exec(db, mergeStmt);
          if (addColumnResult.isErr()) {
            return err(
              `Error adding column ${mergeStmt}: ${addColumnResult.error}`
            );
          }
          break;
      }
    }

    for (const batchRow of inserts) {
      const { columns, values } = batchRow;
      const insertStatement = `insert or replace into ${fullTableName} (${columns.join(
        ", "
      )}) values (${values.map(() => "?").join(", ")})`;
      const tryInsert = await all(db, insertStatement, values);
      if (tryInsert.isErr()) {
        console.error(
          `Insert or replace error: ${insertStatement}`,
          tryInsert.error
        );
        return err(tryInsert.error);
      }
    }

    return ok({
      schemaName,
      tableName,
      columnSchema: mergedColumnSchema,
    });
  }

  async handleNewRecords(event: CreateRecordsEvent) {
    const db = await this.getDb();
    const { inserts } = event;
    const stagedTables = await stageInserts(db, inserts);
    for (const file of stagedTables) {
      const queuedWrite = await this.mergeStagedTable(file);
      if (queuedWrite.isErr()) {
        console.error(
          `Error committing file for ${file.tableName}: ${queuedWrite.error}`
        );
        continue;
      }
      const { schemaName, tableName } = queuedWrite.value;

      const key = `${schemaName}.${tableName}`;
      this.queuedWrites.set(key, queuedWrite.value);

      // Get or create a debounced write function for this table
      let debouncedWrite = this.debouncedWrites.get(key);
      if (!debouncedWrite) {
        debouncedWrite = debounce(this.performWrite.bind(this, key), 3000);
        this.debouncedWrites.set(key, debouncedWrite);
      }

      debouncedWrite();
    }
  }

  async updateStore(dataLocation: DataLocation) {
    const { schemaName, tableName } = dataLocation;
    const existingTable = await this.store.getTable(schemaName, tableName);
    if (existingTable === null) {
      await this.store.addTable(dataLocation);
    }
  }

  private async performWrite(key: string) {
    const queuedWrite = this.queuedWrites.get(key);
    if (queuedWrite) {
      const dataLocation = await this.writeTable(queuedWrite);
      if (dataLocation.isErr()) {
        throw new Error(dataLocation.error);
      }
      this.queuedWrites.delete(key);
      await this.updateStore(dataLocation.value);
    } else {
      console.log("performWrite no queued write", key);
    }
  }

  // async handleNewDataLocation(dataLocation: DataLocation): Promise<void> {
  //   const db = await this.getDb();
  //   await addDuckDBTables(db, [dataLocation]);
  // }

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
