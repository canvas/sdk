import { SecretStore } from "../secrets/secret.store";
import { S3SQLiteStore, SQLiteStore, Store } from "../store/sqlite.store";
import { InMemoryDuckDb, SqlEngine } from "../query/query";
import { LoaderExecutor } from "../loader/loader";
import {
  TransformerInputEvent,
  Loader,
  BaseTransformer,
  TransformType,
  ReadDataLocation,
  S3Credentials,
} from "./types";
import { TransformerExecutor } from "../transformer/transformer";
import { InMemoryBroker } from "../messages/in.memory.broker";
import { Server } from "http";
import { initializeServer } from "../server/server";
import { LocalBackend, S3Backend, Writer } from "../writer/writer";
import { MessageBroker } from "../messages/message.broker";

const port = process.env.PORT || 9001;

type LoaderExtras = {
  rateLimitMs: number;
  cadenceSeconds: number;
};
type BuilderLoader<Secret, Cursor> = {
  loader: Loader<Secret, Cursor>;
  uniqueId: string;
  extras?: LoaderExtras;
};

export class SwampBuilder {
  secrets: SecretStore | null = null;
  s3Credentials: S3Credentials | null = null;
  queryEnabled: boolean = false;
  loaders: BuilderLoader<any, any>[] = [];

  withSecretStore(secrets: SecretStore): SwampBuilder {
    this.secrets = secrets;
    return this;
  }
  withS3Credentials(s3CredentialKey: string): SwampBuilder {
    if (!this.secrets) {
      throw new Error("Must set a secret store");
    }
    const s3Credentials = this.secrets.getS3Credentials(s3CredentialKey);
    this.s3Credentials = s3Credentials;
    return this;
  }
  withQueryEngine(): SwampBuilder {
    this.queryEnabled = true;
    return this;
  }
  withLoader(
    loader: Loader<any, any>,
    uniqueId: string,
    extras?: LoaderExtras
  ): SwampBuilder {
    this.loaders.push({ loader, uniqueId, extras });
    return this;
  }
  initialSwamp(): Swamp {
    if (!this.secrets) {
      throw new Error("Must set a secret store");
    }
    const messageBroker = new InMemoryBroker();
    if (this.s3Credentials) {
      const s3Credentials = this.s3Credentials;
      const store = new S3SQLiteStore(s3Credentials);
      const sqlEngine = this.queryEnabled
        ? new InMemoryDuckDb().withS3Credentials(s3Credentials)
        : null;
      const dataBackend = new S3Backend(s3Credentials);
      const writer = new Writer(store, dataBackend, messageBroker);
      const swamp = new Swamp(
        store,
        this.secrets,
        messageBroker,
        writer,
        sqlEngine
      );
      return swamp;
    } else {
      const store = new SQLiteStore("local.db");
      const sqlEngine = new InMemoryDuckDb();
      const dataBackend = new LocalBackend();
      const writer = new Writer(store, dataBackend, messageBroker);
      const swamp = new Swamp(
        store,
        this.secrets,
        messageBroker,
        writer,
        sqlEngine
      );
      return swamp;
    }
  }
  build(): Swamp {
    const swamp = this.initialSwamp();
    for (const loader of this.loaders) {
      swamp
        .addLoader(loader.loader, loader.uniqueId, loader.extras?.rateLimitMs)
        .withCadence(loader.extras?.cadenceSeconds || 60 * 15);
    }
    swamp.initialize();
    return swamp;
  }
}

export class Swamp {
  store: Store;
  secretStore: SecretStore;
  transformers: BaseTransformer[];
  sqlEngine: SqlEngine | null = null;
  messageBroker: MessageBroker;
  writer: Writer;
  server: Server | null = null;

  constructor(
    store: Store,
    secretStore: SecretStore,
    messageBroker: MessageBroker,
    writer: Writer,
    sqlEngine: SqlEngine | null
  ) {
    this.store = store;
    this.secretStore = secretStore;
    this.transformers = [];
    this.messageBroker = messageBroker;
    this.writer = writer;
    this.sqlEngine = sqlEngine;
  }

  async initialize(): Promise<void> {
    console.log("initialize");
    if (this.sqlEngine) {
      const tables = await this.store.getTables();
      await this.sqlEngine.initialize(tables, this.messageBroker);
    }
    const app = initializeServer(this);
    this.server = app.listen(port, () => {
      console.log(`Listening: http://localhost:${port}`);
    });
  }

  query(query: string) {
    if (!this.sqlEngine) {
      throw new Error("No SqlEngine installed");
    }
    return this.sqlEngine.querySql(query);
  }

  addTransformer(
    transform: TransformType,
    uniqueId: string
  ): TransformerExecutor {
    const transformer = new TransformerExecutor(
      this.secretStore,
      this.messageBroker,
      uniqueId,
      transform
    );
    this.transformers.push(transformer);
    return transformer;
  }

  addLoader<Secret, Cursor>(
    load: Loader<Secret, Cursor>,
    uniqueId: string,
    rateLimitMs?: number
  ): LoaderExecutor<Secret, Cursor> {
    const loader = new LoaderExecutor(
      this.secretStore,
      this.store,
      this.messageBroker,
      uniqueId,
      load,
      rateLimitMs
    );
    this.transformers.push(loader);
    return loader;
  }

  getTransformer(uniqueId: string): BaseTransformer | undefined {
    return this.transformers.find(
      (transformer) => transformer.uniqueId === uniqueId
    );
  }

  async runTransformer(loaderId: string, event: TransformerInputEvent) {
    const transformer = this.getTransformer(loaderId);
    if (!transformer) {
      console.error(`Transformer ${loaderId} not found`);
      return;
    }
    const subscriptions = transformer.subscriptions;
    const tableLocations = await this.store.getTables();
    const subscribedTables: ReadDataLocation[] = [];
    for (const subscription of subscriptions) {
      for (const table of subscription.tables) {
        const tableLocation = tableLocations.find(
          (loc) =>
            loc.tableName === table && loc.schemaName === subscription.uniqueId
        );
        if (tableLocation) {
          subscribedTables.push(tableLocation);
        } else {
          console.log(
            `Table ${subscription.uniqueId}.${table} subscribed by ${loaderId} not found`
          );
          return;
        }
      }
    }
    transformer.execute(event, subscribedTables);
  }
}
