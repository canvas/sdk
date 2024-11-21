import {
  Swamp,
  InMemoryDuckDb,
  EnvVarSecretStore,
  runPostgres,
  SQLiteStore,
} from "@canvas-sdk/swamp";

export function syncPostgres(): Swamp {
  const metaStore = new EnvVarSecretStore();
  const store = new SQLiteStore("local.db");
  const sqlEngine = new InMemoryDuckDb(store);
  const swamp = new Swamp(store, metaStore, sqlEngine);
  swamp.initialize();
  swamp.addLoader(runPostgres, "postgres").withCadence(60 * 60);
  return swamp;
}

syncPostgres();
