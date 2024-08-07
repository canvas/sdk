import { BaseTransformer, TransformerInputEvent, TransformType, Inserts, TransformSQL, RawLoaderInserts, DataLocation } from '../core/types';
import { MessageBroker } from '../messages/message.broker';
import { initializeWithTables } from '../query/duckdb';
import { all } from '../query/duckdb.util';
import { SecretStore } from '../secrets/secret.store';
import { processInserts } from '../util/record.util';

export type TableRegistration = {
  schemaName: string;
};

export class TransformerExecutor extends BaseTransformer {

  transform: TransformType;

  messageBroker: MessageBroker;

  transformType = 'transform' as const;

  secretStore: SecretStore;

  constructor(secretStore: SecretStore, messageBroker: MessageBroker, uniqueId: string, transformer: TransformType) {
    super(uniqueId);
    this.uniqueId = uniqueId;
    this.transform = transformer;
    this.messageBroker = messageBroker;
    this.secretStore = secretStore;
  }

  async execute(event: TransformerInputEvent, locations: DataLocation[]): Promise<void> {
    try {
      switch (event.type) {
        case 'records':
          await this.executeIncremental(event.inserts, locations);
          break;
        case 'run':
          await this.executeFull(locations);
      }
    } catch (e) {
      console.error(e);
    }
  }

  async executeFull(dependentTables: DataLocation[]): Promise<void> {
    if (this.transform.type === 'sql') {
      const rawInserts = await this.executeSql(this.transform, dependentTables);
      const inserts = processInserts(rawInserts, this.uniqueId);
      this.messageBroker.publishNewRecords({ type: 'records', inserts });
    }
  }

  async executeIncremental(incomingInserts: RawLoaderInserts, dependentTables: DataLocation[]): Promise<void> {
    if (this.transform.type === 'sql') {
      const rawInserts = await this.executeSql(this.transform, dependentTables);
      const inserts = processInserts(rawInserts, this.uniqueId);
      this.messageBroker.publishNewRecords({ type: 'records', inserts });
    }
  }

  async executeSql(transform: TransformSQL, dependentTables: DataLocation[]): Promise<Inserts>  {
    const tryTransform = transform();
    if (tryTransform.isErr()) {
      console.error(tryTransform.error);
      return {};
    }
    const { query, tableName, primaryKey } = tryTransform.value;
    const database = await initializeWithTables(dependentTables);
    const records = await all(database, query);
    if (records.isErr()) {
      console.error(`Error with transform ${this.uniqueId}: ${records.error}`);
      return {}; 
    }
    return { [tableName]: { records: records.value, primaryKeys: [primaryKey] } };
  }
}