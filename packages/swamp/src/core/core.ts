import { SecretStore } from "../secrets/secret.store";
import { S3SQLiteStore, SQLiteStore, Store } from "../store/sqlite.store";
import { DuckDbEngine, SqlEngine } from "../query/query";
import { LoaderExecutor } from "../loader/loader";
import {
  TransformerInputEvent,
  Loader,
  BaseTransformer,
  TransformType,
  S3Credentials,
} from "./types";
import { TransformerExecutor } from "../transformer/transformer";
import { InMemoryBroker } from "../messages/in.memory.broker";
import { Server } from "http";
import { initializeServer } from "../server/server";
import { MessageBroker } from "../messages/message.broker";

// Fixes Express Do not know how to serialize a BigInt
(BigInt.prototype as any).toJSON = function () {
  return this.toString();
};

const port = process.env.PORT || 9001;

type LoaderExtras = {
  rateLimitMs?: number;
  cadenceSeconds?: number;
};
type BuilderLoader<Secret, Cursor> = {
  loader: Loader<Secret, Cursor>;
  uniqueId: string;
  extras?: LoaderExtras;
};
type BuilderTransformer = {
  transformer: TransformType;
  uniqueId: string;
};

export class SwampBuilder {
  secrets: SecretStore | null = null;
  s3Credentials: S3Credentials | null = null;
  loaders: BuilderLoader<any, any>[] = [];
  transformers: BuilderTransformer[] = [];

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
  withLoader(
    loader: Loader<any, any>,
    uniqueId: string,
    extras?: LoaderExtras
  ): SwampBuilder {
    this.loaders.push({ loader, uniqueId, extras });
    return this;
  }
  withTransformer(transformer: TransformType, uniqueId: string): SwampBuilder {
    this.transformers.push({ transformer, uniqueId });
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
      const sqlEngine = new DuckDbEngine(
        store,
        messageBroker
      ).withS3Credentials(s3Credentials);
      const swamp = new Swamp(store, this.secrets, messageBroker, sqlEngine);
      return swamp;
    } else {
      const store = new SQLiteStore("local.db");
      const sqlEngine = new DuckDbEngine(store, messageBroker);
      const swamp = new Swamp(store, this.secrets, messageBroker, sqlEngine);
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
    for (const transformer of this.transformers) {
      swamp.addTransformer(transformer.transformer, transformer.uniqueId);
    }
    swamp.initialize();
    return swamp;
  }
}

export class Swamp {
  store: Store;
  secretStore: SecretStore;
  transformers: BaseTransformer[];
  sqlEngine: SqlEngine;
  messageBroker: MessageBroker;
  server: Server | null = null;

  constructor(
    store: Store,
    secretStore: SecretStore,
    messageBroker: MessageBroker,
    sqlEngine: SqlEngine
  ) {
    this.store = store;
    this.secretStore = secretStore;
    this.transformers = [];
    this.messageBroker = messageBroker;
    this.sqlEngine = sqlEngine;
  }

  async initialize(): Promise<void> {
    await this.sqlEngine.initialize();
    if (process.env.NODE_ENV === "development") {
      for (const transformer of this.transformers) {
        if (transformer.transformType === "transform") {
          console.log(`Running transformer ${transformer.uniqueId}`);
          transformer.execute({ type: "run", force: true });
        }
      }
    }
    const app = initializeServer(this);
    this.server = app.listen(port, () => {
      console.log(`Listening: http://localhost:${port}`);
    });
  }

  query(query: string) {
    return this.sqlEngine.querySql(query);
  }

  addTransformer(
    transform: TransformType,
    uniqueId: string
  ): TransformerExecutor {
    const transformer = new TransformerExecutor(
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
    transformer.execute(event);
  }
}
