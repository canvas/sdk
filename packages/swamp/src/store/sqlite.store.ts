import { Database as SQLiteDatabase } from "better-sqlite3";
import Database from "better-sqlite3";
import { z } from "zod";
import { ColumnSchema, DataLocation, S3Credentials } from "../core/types";
import { fetchFileFromS3, uploadFileToS3 } from "../util";
import debounce from "debounce";
import { InMemoryLock } from "../lock";

const InnerTableObject = z.object({
  tableName: z.string(),
  schemaName: z.string(),
  location: z.string(),
  dataType: z.union([z.literal("json"), z.literal("parquet")]),
  fileLocation: z.union([z.literal("local"), z.literal("s3")]),
  columnSchema: z.string(),
});
export type InnerTableObject = z.infer<typeof InnerTableObject>;

export const LoaderRunInput = z.object({
  uniqueId: z.string(),
  status: z.union([z.literal("success"), z.literal("error")]),
  message: z.string().optional(),
});
export type LoaderRunInput = z.infer<typeof LoaderRunInput>;

export const LoaderRunOutput = z.object({
  status: z.union([z.literal("success"), z.literal("error")]),
  message: z.string().optional(),
  createdAt: z.string().datetime(),
});
export type LoaderRunOutput = z.infer<typeof LoaderRunOutput>;

export type Store = {
  addCursor: (uniqueId: string, cursor: Record<string, any>) => Promise<void>;
  getCursor: (uniqueId: string) => Promise<Record<string, any> | null>;
  addRun(run: LoaderRunInput): Promise<void>;
  getLatestRun(uniqueId: string): Promise<LoaderRunOutput | null>;
} & TableStoreInterface;

export type TableStoreInterface = {
  addTable: (tableObject: DataLocation) => Promise<void>;
  getTables: (uniqueId?: string) => Promise<DataLocation[]>;
  getTable: (schema: string, table: string) => Promise<DataLocation | null>;
};

const CATLOG_TABLE_NAME = "catalog_table";
const CURSOR_TABLE_NAME = "loader_cursor";
const RUN_TABLE_NAME = "loader_run";

const CREATE_TABLE_STATEMENT = `
CREATE TABLE IF NOT EXISTS ${CATLOG_TABLE_NAME} (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
  tableName TEXT NOT NULL,
  schemaName TEXT NOT NULL,
  dataType TEXT NOT NULL,
  location TEXT UNIQUE NOT NULL,
  columnSchema TEXT NOT NULL,
  fileLocation TEXT NOT NULL CHECK(fileLocation IN ('local', 's3')),
  UNIQUE (tableName, schemaName) ON CONFLICT REPLACE
);`;

const CREATE_CURSOR_STATEMENT = `
CREATE TABLE IF NOT EXISTS ${CURSOR_TABLE_NAME} (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
  uniqueId TEXT NOT NULL UNIQUE,
  cursor TEXT NOT NULL
);`;

const CREATE_RUN_STATEMENT = `
CREATE TABLE IF NOT EXISTS ${RUN_TABLE_NAME} (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
  uniqueId TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('success', 'error')),
  message TEXT
);`;

const SETUP_STATEMENTS = [
  CREATE_TABLE_STATEMENT,
  CREATE_CURSOR_STATEMENT,
  CREATE_RUN_STATEMENT,
];

function addTable(db: SQLiteDatabase, table: DataLocation): void {
  const {
    tableName,
    schemaName,
    location,
    dataType,
    columnSchema,
    fileLocation,
  } = table;
  db.prepare(
    `
    INSERT OR REPLACE INTO ${CATLOG_TABLE_NAME} (tableName, schemaName, location, dataType, columnSchema, fileLocation) 
      VALUES (?, ?, ?, ?, ?, ?)
    `
  ).run(
    tableName,
    schemaName,
    location,
    dataType,
    JSON.stringify(columnSchema),
    fileLocation
  );
}

function getTable(
  db: SQLiteDatabase,
  schemaName: string,
  tableName: string
): DataLocation | null {
  const result = db
    .prepare<{}, InnerTableObject>(
      `SELECT * FROM ${CATLOG_TABLE_NAME} WHERE schemaName = ? AND tableName = ?`
    )
    .get([schemaName, tableName]);
  if (result) {
    return {
      ...result,
      columnSchema: JSON.parse(result.columnSchema),
    };
  }
  return result || null;
}

function getTables(db: SQLiteDatabase, uniqueId?: string): DataLocation[] {
  if (uniqueId) {
    const tables = db
      .prepare<{}, InnerTableObject>(
        `SELECT * FROM ${CATLOG_TABLE_NAME} where schemaName = ?`
      )
      .all(uniqueId);
    return tables.map((table) => ({
      ...table,
      columnSchema: JSON.parse(table.columnSchema),
    }));
  } else {
    const tables = db
      .prepare<{}, InnerTableObject>(`SELECT * FROM ${CATLOG_TABLE_NAME}`)
      .all({});
    return tables.map((table) => ({
      ...table,
      columnSchema: JSON.parse(table.columnSchema),
    }));
  }
}

function addCursor(
  db: SQLiteDatabase,
  uniqueId: string,
  cursor: Record<string, any>
): void {
  db.prepare(
    `
    INSERT OR REPLACE INTO ${CURSOR_TABLE_NAME} (uniqueId, cursor) 
      VALUES (?, ?)
    `
  ).run(uniqueId, JSON.stringify(cursor));
}

function getCursor(
  db: SQLiteDatabase,
  uniqueId: string
): Record<string, any> | null {
  const result = db
    .prepare<{}, { cursor: string }>(
      `SELECT cursor FROM ${CURSOR_TABLE_NAME} WHERE uniqueId = ?`
    )
    .get(uniqueId);
  if (result) {
    return JSON.parse(result.cursor);
  }
  return null;
}

function addRun(db: SQLiteDatabase, run: LoaderRunInput): void {
  db.prepare(
    `
    INSERT INTO ${RUN_TABLE_NAME} (uniqueId, status, message) 
      VALUES (?, ?, ?)
    `
  ).run(run.uniqueId, run.status, run.message);
}

function getLatestRun(
  db: SQLiteDatabase,
  uniqueId: string
): LoaderRunOutput | null {
  const result = db
    .prepare<{}, LoaderRunOutput>(
      `SELECT status, message, createdAt FROM ${RUN_TABLE_NAME} WHERE uniqueId = ? ORDER BY createdAt DESC LIMIT 1`
    )
    .get(uniqueId);
  return result || null;
}

export class SQLiteStore implements Store {
  db: SQLiteDatabase;

  constructor(fileName: string) {
    this.db = new Database(fileName);
    for (const statement of SETUP_STATEMENTS) {
      this.db.exec(statement);
    }
  }

  async addTable(table: DataLocation): Promise<void> {
    addTable(this.db, table);
  }

  async getTable(
    schemaName: string,
    tableName: string
  ): Promise<DataLocation | null> {
    return getTable(this.db, schemaName, tableName);
  }

  async getTables(uniqueId?: string): Promise<DataLocation[]> {
    return getTables(this.db, uniqueId);
  }

  async addCursor(
    uniqueId: string,
    cursor: Record<string, any>
  ): Promise<void> {
    return addCursor(this.db, uniqueId, cursor);
  }

  async getCursor(uniqueId: string): Promise<Record<string, any> | null> {
    return getCursor(this.db, uniqueId);
  }

  async addRun(run: LoaderRunInput): Promise<void> {
    return addRun(this.db, run);
  }

  async getLatestRun(uniqueId: string): Promise<LoaderRunOutput | null> {
    return getLatestRun(this.db, uniqueId);
  }
}

export class S3SQLiteStore implements Store {
  credentials: S3Credentials;

  fileName: string;

  _db: SQLiteDatabase | null = null;

  lock = new InMemoryLock();

  debouncedUpload: () => void;

  constructor(credentials: S3Credentials, fileName: string = "local.db") {
    this.credentials = credentials;
    this.fileName = fileName;
    this.debouncedUpload = debounce(this.uploadToS3.bind(this), 1000);
  }

  async db(): Promise<SQLiteDatabase> {
    await this.lock.acquire();
    try {
      if (!this._db) {
        const path = await fetchFileFromS3(this.credentials, this.fileName);
        if (path) {
          console.log("Restored existing database from s3");
          this._db = new Database(path);
        } else {
          console.log("Creating new database");
          this._db = new Database(this.fileName);
          for (const statement of SETUP_STATEMENTS) {
            this._db.exec(statement);
          }
          await this.writeRemote();
        }
      }
      return this._db;
    } finally {
      this.lock.release();
    }
  }

  uploadToS3() {
    uploadFileToS3(this.credentials, this.fileName, this.fileName);
  }

  async writeRemote(): Promise<void> {
    this.debouncedUpload();
  }

  async addTable(table: DataLocation): Promise<void> {
    const result = addTable(await this.db(), table);
    await this.writeRemote();
    return result;
  }

  async getTable(
    schemaName: string,
    tableName: string
  ): Promise<DataLocation | null> {
    return getTable(await this.db(), schemaName, tableName);
  }

  async getTables(uniqueId?: string): Promise<DataLocation[]> {
    return getTables(await this.db(), uniqueId);
  }

  async addCursor(
    uniqueId: string,
    cursor: Record<string, any>
  ): Promise<void> {
    const result = addCursor(await this.db(), uniqueId, cursor);
    await this.writeRemote();
    return result;
  }

  async getCursor(uniqueId: string): Promise<Record<string, any> | null> {
    return getCursor(await this.db(), uniqueId);
  }

  async addRun(run: LoaderRunInput): Promise<void> {
    const result = addRun(await this.db(), run);
    await this.writeRemote();
    return result;
  }

  async getLatestRun(uniqueId: string): Promise<LoaderRunOutput | null> {
    return getLatestRun(await this.db(), uniqueId);
  }
}
