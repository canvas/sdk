import { Result } from "neverthrow";
import { z } from "zod";
import { SqlEngine } from "../query/query";

export const S3Credentials = z.object({
  keyId: z.string(),
  secret: z.string(),
  region: z.string(),
  bucket: z.string(),
});
export type S3Credentials = z.infer<typeof S3Credentials>;

export type FileDataType = "json" | "parquet";

export type FileLocation = "local" | "s3";

export const DuckDBRawType = z.enum([
  "VARCHAR",
  "DOUBLE",
  "DATE",
  "TIMESTAMP",
  "JSON",
  "BOOLEAN",
  "BIGINT",
  "BLOB",
]);
export type DuckDBRawType = z.infer<typeof DuckDBRawType>;

export const DuckDBType = z.union([
  z.object({
    type: DuckDBRawType,
  }),
  z.object({
    type: z.literal("ARRAY"),
    innerType: DuckDBRawType,
  }),
]);
export type DuckDBType = z.infer<typeof DuckDBType>;

export const ColumnSchema = z.object({
  columnType: DuckDBType,
  isPrimaryKey: z.boolean(),
});
export type ColumnSchema = z.infer<typeof ColumnSchema>;
export const ColumnsSchema = z.record(ColumnSchema);
export type ColumnsSchema = z.infer<typeof ColumnsSchema>;

export type DataLocation = {
  dataType: FileDataType;
  fileLocation: FileLocation;
  location: string;
  tableName: string;
  schemaName: string;
  columnSchema: Record<string, ColumnSchema>;
};

type Run = { type: "run"; force: boolean };

type RecordBatch = {
  primaryKeys: string[];
  records: Record<string, any>[];
};

export type Inserts = Record<string, RecordBatch>;

type RawLoaderInsert = {
  records: Record<string, any>[];
  columnSchema: ColumnsSchema;
};
export type RawLoaderInserts = Record<string, RawLoaderInsert>;

type LoaderInsert = RawLoaderInsert & { schemaName: string };
export type LoaderInserts = Record<string, LoaderInsert>;

export type InsertRecordsEvent = {
  type: "records";
  inserts: LoaderInserts;
};

export type CreateSqlTableEvent = {
  type: "sql_table";
} & TransformSQLResult;

export type WriteRecordsEvent = InsertRecordsEvent | CreateSqlTableEvent;

export type TableUpdatedEvent = {
  type: "table_updated";
  schemaName: string;
  tableName: string;
};

export type TransformerInputEvent = InsertRecordsEvent | Run;

const LoaderSecrets = z.record(z.string()).nullable();

export type LoaderResponse<CursorType> =
  | {
      type: "success";
      cursor: CursorType;
      inserts: Inserts;
      hasMore: boolean;
    }
  | {
      type: "error";
      message: string;
    };
export type LoaderSecrets = z.infer<typeof LoaderSecrets>;

export type Loader<SecretsType, CursorType> = {
  (secrets: SecretsType | null, cursor: CursorType | null): Promise<
    LoaderResponse<CursorType>
  >;
  cursor: z.AnyZodObject;
  secrets: z.AnyZodObject | null;
};

export type TransformSQLResult = {
  query: string;
  tableName: string;
  schemaName: string;
  primaryKeys: string[];
};
export type TransformSQL = {
  (): TransformSQLResult;
  type: "sql";
  subscriptions: { schemaName: string; tableName: string }[];
};

export type TransformType = TransformSQL;

export type SubscriptionType = {
  uniqueId: string;
  tables: string[];
};

export type ReadDataLocation = {
  dataType: FileDataType;
  fileLocation: FileLocation;
  location: string;
  tableName: string;
  schemaName: string;
};

export abstract class BaseTransformer {
  uniqueId: string;

  subscriptions: SubscriptionType[];

  cadenceSeconds: number;

  filePath: string | null;

  abstract readonly transformType: "load" | "transform";

  constructor(uniqueId: string) {
    this.uniqueId = uniqueId;
    this.subscriptions = [];
    this.cadenceSeconds = 60 * 15;
    this.filePath = null;
  }

  abstract execute(event: TransformerInputEvent): void;

  withSubscription(transformer: BaseTransformer, tables: string[]) {
    this.subscriptions.push({ uniqueId: transformer.uniqueId, tables });
    return this;
  }

  withCadence(cadenceSeconds: number) {
    this.cadenceSeconds = cadenceSeconds;
    return this;
  }

  setFilePath(filePath: string) {
    this.filePath = filePath;
    return this;
  }
}
