import { Result, err, ok } from "neverthrow";
import {
  ColumnSchema,
  ColumnsSchema,
  CreateRecordsEvent,
  DataLocation,
  LoaderInserts,
  S3Credentials,
} from "../core/types";
import {
  generateRandomAlphanumeric,
  getTempDirectory,
  uploadFileToS3,
} from "../util";
import { all, DUCKDB_KEYWORDS } from "../query/duckdb.util";
import { Database } from "duckdb";
import fs from "fs";
import { TableStoreInterface } from "../store/sqlite.store";
import { MessageBroker } from "../messages/message.broker";

type StagedFile = {
  dataType: "parquet";
  tableName: string;
  schemaName: string;
  columnSchema: ColumnsSchema;
  queuedFilePath: string;
};

export class Writer {
  store: TableStoreInterface;
  backend: DataBackend;
  messageBroker: MessageBroker;
  constructor(
    store: TableStoreInterface,
    backend: DataBackend,
    messageBroker: MessageBroker
  ) {
    this.store = store;
    this.backend = backend;
    this.messageBroker = messageBroker;
    messageBroker.subscribeToNewRecords(this.handleNewRecords.bind(this));
  }

  async handleNewRecords(event: CreateRecordsEvent) {
    const { inserts } = event;
    const stagedFiles = await getInsertParquet(inserts);
    for (const file of stagedFiles) {
      const dataLocation = await this.backend.commitFiles(file);
      if (dataLocation.isErr()) {
        console.error(`Error committing file: ${dataLocation.error}`);
        continue;
      }
      this.messageBroker.publishNewDataLocation(dataLocation.value);
    }
  }

  async updateStore(dataLocation: DataLocation) {
    const { schemaName, tableName } = dataLocation;
    const existingTable = await this.store.getTable(schemaName, tableName);
    if (existingTable === null) {
      await this.store.addTable(dataLocation);
    }
  }
}

const DATA_ROOT = "data";

export interface DataBackend {
  commitFiles(staged: StagedFile): Promise<Result<DataLocation, string>>;
}

export class LocalBackend implements DataBackend {
  async commitFiles(staged: StagedFile): Promise<Result<DataLocation, string>> {
    const { tableName, schemaName, columnSchema, queuedFilePath } = staged;
    const folderLocation = `${DATA_ROOT}/${schemaName}/${tableName}`;
    fs.mkdirSync(folderLocation, { recursive: true });
    const fileLocation = `${folderLocation}/${getFileName()}`;
    fs.renameSync(queuedFilePath, fileLocation);

    return ok({
      location: folderLocation,
      tableName,
      schemaName,
      columnSchema,
      dataType: "parquet" as const,
      fileLocation: "local",
    });
  }
}

export class S3Backend implements DataBackend {
  credentials: S3Credentials;
  constructor(credentials: S3Credentials) {
    this.credentials = credentials;
  }
  async commitFiles(staged: StagedFile): Promise<Result<DataLocation, string>> {
    const { tableName, schemaName, columnSchema, queuedFilePath } = staged;

    const folderLocation = `${DATA_ROOT}/${schemaName}/${tableName}`;
    const fileLocation = `${folderLocation}/${getFileName()}`;

    const tryUpload = await uploadFileToS3(
      this.credentials,
      queuedFilePath,
      fileLocation
    );
    fs.unlinkSync(queuedFilePath);
    if (tryUpload.isErr()) {
      console.error(`Error uploading to S3: ${tryUpload.error}`);
      return err(tryUpload.error);
    }

    return ok({
      location: `s3://${this.credentials.bucket}/${folderLocation}`,
      tableName,
      schemaName,
      columnSchema,
      dataType: "parquet",
      fileLocation: "s3",
    });
  }
}

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
  records: Record<string, any>[],
  schema: Record<string, ColumnSchema>,
  writePath: string
): Promise<Result<CreateBatchTableResult, string>> {
  const db = new Database(":memory:");
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

function getFileName(): string {
  return `${new Date().getTime()}-${generateRandomAlphanumeric(4)}.parquet`;
}

export async function getInsertParquet(
  inserts: LoaderInserts
): Promise<StagedFile[]> {
  const queuedWrites: StagedFile[] = [];
  for (const [tableName, batch] of Object.entries(inserts)) {
    const { columnSchema, records, schemaName } = batch;
    if (records.length === 0) {
      continue;
    }

    const writeFolder = getTempDirectory();

    const fileName = getFileName();
    const writePath = `${writeFolder}/${fileName}`;

    const parquetFile = await createBatchTable(
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

    const queuedWrite: StagedFile = {
      dataType: "parquet" as const,
      tableName,
      schemaName,
      columnSchema,
      queuedFilePath: parquetFile.value.filePath,
    };
    queuedWrites.push(queuedWrite);
  }
  return queuedWrites;
}
