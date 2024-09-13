import { ColumnInfo, TableData, Database } from "duckdb";
import { TableStoreInterface } from "../store/sqlite.store";
import {
  createS3SecretStatement,
  fetchFileFromS3,
  uploadFileToS3,
} from "../util";
import {
  ColumnSchema,
  CreateSqlTableEvent,
  DataLocation,
  DuckDBType,
  InsertRecordsEvent,
  S3Credentials,
  WriteRecordsEvent,
} from "../core/types";
import {
  all,
  exec,
  generateCreateTableStatement,
  flattenInserts,
  prepare,
  InsertRows,
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
  initialize(): Promise<void>;
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
};

function mergeSchemas(
  existingSchema: Record<string, ColumnSchema>,
  newSchema: Record<string, ColumnSchema>
): MergeResult {
  const schemaChanges: SchemaChange[] = [];
  const columnSchema: Record<string, ColumnSchema> = existingSchema;
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

  return { schemaChanges: schemaChanges, columnSchema };
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
      const { createSchemaStatement, createTableStatement, fullTableName } =
        generateCreateTableStatement(
          table.tableName,
          table.schemaName,
          table.columnSchema,
          false
        );
      return `
      ${createSchemaStatement};
      ${createTableStatement};
      INSERT INTO ${fullTableName}
        SELECT * FROM read_parquet('${table.location}');`;
    })
    .join(";");
  const result = await all(db, createTableStatements);
  if (result.isErr()) {
    console.log("Error adding table", result.error);
  }
  return db;
}

const DATA_ROOT = "data";

export class DuckDbEngine implements SqlEngine {
  db: Database | null = null;
  credentials: S3Credentials | null = null;
  store: TableStoreInterface;
  messageBroker: MessageBroker;

  private queuedWrites: Map<string, QueuedWrite> = new Map();
  private debouncedWrites: Map<string, ReturnType<typeof debounce>> = new Map();

  constructor(store: TableStoreInterface, messageBroker: MessageBroker) {
    this.store = store;
    this.messageBroker = messageBroker;
  }

  withS3Credentials(credentials: S3Credentials): DuckDbEngine {
    this.credentials = credentials;
    return this;
  }

  async initialize(): Promise<void> {
    const localTableLocations = await this.downloadData();
    await addDuckDBTables(await this.getDb(), localTableLocations);
    this.messageBroker.subscribeToNewWrites(this.handleWriteRecords.bind(this));
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
      }
      return ok({
        ...queuedWrite,
        location: tryUpload.value,
        dataType: "parquet",
        fileLocation: "s3",
      });
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
    stagedTable: InsertRows
  ): Promise<Result<QueuedWrite, string>> {
    const db = await this.getDb();
    const {
      rows: inserts,
      columnSchema: newColumnSchema,
      schemaName,
      tableName,
    } = stagedTable;

    const existingTable = await this.getTable(schemaName, tableName);

    const { createSchemaStatement, createTableStatement, fullTableName } =
      generateCreateTableStatement(
        tableName,
        schemaName,
        newColumnSchema,
        false
      );

    if (!existingTable) {
      // must create a new table in order to preserve primary keys

      const createTableStmt = `${createSchemaStatement};
      ${createTableStatement};`;
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

    const { schemaChanges: actions, columnSchema: mergedColumnSchema } =
      mergeSchemas(existingColumnSchema, newColumnSchema);

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

  async handleInsertRecords(
    event: InsertRecordsEvent
  ): Promise<Result<QueuedWrite[], string>> {
    const { inserts } = event;
    const tablesRows = await flattenInserts(inserts);
    const queuedWrites: QueuedWrite[] = [];
    for (const tableRows of tablesRows) {
      const queuedWrite = await this.mergeStagedTable(tableRows);
      if (queuedWrite.isErr()) {
        console.error(
          `Error committing file for ${tableRows.tableName}: ${queuedWrite.error}`
        );
        continue;
      }
      queuedWrites.push(queuedWrite.value);
    }
    return ok(queuedWrites);
  }

  async handleCreateSqlTable(
    event: CreateSqlTableEvent
  ): Promise<Result<QueuedWrite, string>> {
    const { schemaName, tableName, query, primaryKeys } = event;
    // First, get the columnSchema from the query
    const tryColumnSchema = await this.getColumnSchemaFromQuery(
      query,
      primaryKeys
    );
    if (tryColumnSchema.isErr()) {
      return err(tryColumnSchema.error);
    }
    const columnSchema = tryColumnSchema.value;

    const { createSchemaStatement, createTableStatement, fullTableName } =
      generateCreateTableStatement(tableName, schemaName, columnSchema, true);

    const insertStatement = `INSERT INTO ${fullTableName} ${query}`;

    const fullStatement = `BEGIN TRANSACTION;
    ${createSchemaStatement};
    ${createTableStatement};
    ${insertStatement};
    COMMIT;`;

    const db = await this.getDb();
    const result = await all(db, fullStatement);
    if (result.isErr()) {
      console.error(
        `Error creating table from query ${fullStatement}: ${result.error}`
      );
      return err(result.error);
    }
    return ok({
      schemaName,
      tableName,
      columnSchema,
    });
  }

  async getColumnSchemaFromQuery(
    query: string,
    primaryKeys: string[]
  ): Promise<Result<Record<string, ColumnSchema>, string>> {
    const db = await this.getDb();
    const statement = await prepare(db, query);
    if (statement.isErr()) {
      return err(statement.error);
    }
    const columns = statement.value.columns();
    const columnSchema: Record<string, ColumnSchema> = {};
    for (const column of columns) {
      columnSchema[column.name] = {
        columnType: { type: column.type.sql_type as any },
        isPrimaryKey: primaryKeys.includes(column.name),
      };
    }
    return ok(columnSchema);
  }

  queueWrite(queuedWrite: QueuedWrite) {
    const { schemaName, tableName } = queuedWrite;
    const key = `${schemaName}.${tableName}`;
    this.queuedWrites.set(key, queuedWrite);
    // Get or create a debounced write function for this table
    let debouncedWrite = this.debouncedWrites.get(key);
    if (!debouncedWrite) {
      debouncedWrite = debounce(this.performWrite.bind(this, key), 3000);
      this.debouncedWrites.set(key, debouncedWrite);
    }
    debouncedWrite();
  }

  async handleWriteRecords(event: WriteRecordsEvent) {
    if (event.type === "records") {
      const writes = await this.handleInsertRecords(event);
      if (writes.isOk()) {
        for (const write of writes.value) {
          this.queueWrite(write);
        }
      } else {
        console.error(`Error handling insert records: ${writes.error}`);
      }
    } else if (event.type === "sql_table") {
      const write = await this.handleCreateSqlTable(event);
      if (write.isOk()) {
        this.queueWrite(write.value);
      } else {
        console.error(`Error handling create sql table: ${write.error}`);
      }
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
      this.messageBroker.publishTableUpdated({
        type: "table_updated",
        schemaName: dataLocation.value.schemaName,
        tableName: dataLocation.value.tableName,
      });
    } else {
      console.error("performWrite no queued write", key);
    }
  }

  async getDb(): Promise<Database> {
    if (!this.db) {
      const db = new Database(":memory:");
      if (this.credentials) {
        await all(db, createS3SecretStatement(this.credentials));
      }
      this.db = db;
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
