import { BaseTransformer, Loader, TransformerInputEvent } from "../core/types";
import { sleep } from "../util";
import { SecretStore } from "../secrets/secret.store";
import { Store } from "../store/sqlite.store";
import { processInserts } from "../util/record.util";
import { InMemoryLock } from "../lock";
import { MessageBroker } from "../messages/message.broker";

export class LoaderExecutor<SecretsType, CursorType> extends BaseTransformer {
  loader: Loader<SecretsType, CursorType>;

  rateLimitMs?: number;

  secretStore: SecretStore;

  store: Store;

  messageBroker: MessageBroker;

  transformType = "load" as const;

  lock = new InMemoryLock();

  constructor(
    secretStore: SecretStore,
    store: Store,
    messageBroker: MessageBroker,
    uniqueId: string,
    loader: Loader<SecretsType, CursorType>,
    rateLimitMs?: number
  ) {
    super(uniqueId);
    this.loader = loader;
    this.uniqueId = uniqueId;
    this.rateLimitMs = rateLimitMs;
    this.secretStore = secretStore;
    this.store = store;
    this.messageBroker = messageBroker;
    this.runLoaderLoop();
  }

  async runLoaderLoop() {
    while (true) {
      await this.maybeRun(false);
      await sleep(1000);
      continue;
    }
  }

  getSecrets(): SecretsType {
    if (!this.loader.secrets) {
      return null as SecretsType;
    }
    const secrets = this.secretStore.get(this.uniqueId, this.loader.secrets);
    if (secrets.isErr()) {
      console.error(
        `Error getting secrets for ${this.uniqueId}: ${secrets.error}`
      );
      throw new Error(
        `Error getting secrets for ${this.uniqueId}: ${secrets.error}`
      );
    }
    return secrets.value as SecretsType;
  }

  async setCursor(cursor: CursorType): Promise<CursorType | null> {
    await this.store.addCursor(this.uniqueId, cursor as any);
    return cursor;
  }

  async getCursor(): Promise<CursorType | null> {
    const cursorRaw = await this.store.getCursor(this.uniqueId);
    if (!cursorRaw) return null;
    const cursor = this.loader.cursor.safeParse(cursorRaw);
    if (cursor.error) {
      throw new Error(
        `Cursor ${JSON.stringify(cursorRaw)} did not match schema: ${
          cursor.error.message
        }`
      );
    }
    return cursor.data as CursorType;
  }

  async execute(event: TransformerInputEvent): Promise<void> {
    switch (event.type) {
      case "records":
        break;
      case "run":
        this.maybeRun(event.force);
    }
  }

  async maybeRun(force: boolean) {
    if (this.lock.isLocked()) {
      console.log("Already running");
      return;
    }
    await this.lock.acquire();
    try {
      const latestRun = await this.store.getLatestRun(this.uniqueId);

      if (!latestRun) {
        await this.executeLoader();
        return;
      }

      if (force) {
        await this.executeLoader();
        return;
      }

      const secondsElapsed =
        (new Date().getTime() - new Date(latestRun.createdAt).getTime()) / 1000;
      if (
        latestRun.status === "success" &&
        secondsElapsed < this.cadenceSeconds
      ) {
        return;
      }
      await this.executeLoader();
    } finally {
      this.lock.release();
    }
  }

  async executeLoader(): Promise<void> {
    const state: CursorType | null = await this.getCursor();
    const secrets = this.getSecrets();
    let cursor = state;
    console.log(
      `Running loader ${this.uniqueId} with state ${JSON.stringify(cursor)}`
    );
    try {
      while (true) {
        const result = await this.loader(secrets, cursor);
        if (result.type === "error") {
          console.error(
            `Error running loader ${this.uniqueId}`,
            result.message
          );
          await this.store.addRun({ uniqueId: this.uniqueId, status: "error" });
          return;
        }
        const inserts = processInserts(result.inserts, this.uniqueId);
        this.messageBroker.publishNewRecords({ type: "records", inserts });
        cursor = result.cursor;
        await this.setCursor(cursor);
        if (result.hasMore) {
          if (this.rateLimitMs) {
            await sleep(this.rateLimitMs);
          }
        } else {
          console.log("Sync complete");
          await this.store.addRun({
            uniqueId: this.uniqueId,
            status: "success",
          });
          return;
        }
      }
    } catch (e) {
      console.error(`Error running loader ${this.uniqueId}`, e);
      await this.store.addRun({ uniqueId: this.uniqueId, status: "error" });
    }
  }
}
