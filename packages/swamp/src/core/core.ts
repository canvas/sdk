import { SecretStore } from "../secrets/secret.store";
import { Store } from "../store/sqlite.store";
import { SqlEngine } from "../query/query";
import { LoaderExecutor } from "../loader/loader";
import {
  TransformerInputEvent,
  Loader,
  BaseTransformer,
  TransformType,
  ReadDataLocation,
} from "./types";
import { TransformerExecutor } from "../transformer/transformer";
import path from "path";
import { InMemoryBroker } from "../messages/in.memory.broker";
import { Server } from "http";
import { initializeServer } from "../server/server";

const port = process.env.PORT || 9001;

export class Swamp {
  store: Store;

  secretStore: SecretStore;

  transformers: BaseTransformer[];

  sqlEngine: SqlEngine;

  messageBroker = new InMemoryBroker();

  server: Server | null = null;

  constructor(store: Store, secretStore: SecretStore, sqlEngine: SqlEngine) {
    this.store = store;
    this.secretStore = secretStore;
    this.transformers = [];
    this.sqlEngine = sqlEngine;
  }

  runChangedTransformer(): string | undefined {
    const args = process.argv.slice(2);
    if (args.length === 0) {
      return;
    }
    const changedRelativePath = args[0];
    const transformer = this.transformers.find(
      (tf) => tf.filePath === changedRelativePath
    );
    if (transformer) {
      this.runTransformer(transformer.uniqueId, { type: "run", force: true });
      return transformer.uniqueId;
    }
  }

  async initialize(): Promise<void> {
    await this.sqlEngine.initialize(this.messageBroker);
    this.runChangedTransformer();
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
      this.secretStore,
      this.messageBroker,
      uniqueId,
      transform
    );
    if (transform.filePath) {
      const parentDir = path.join(__dirname, "../..");
      const relativePath = transform.filePath.replace(parentDir + "/", "");
      transformer.setFilePath(relativePath);
    }
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
