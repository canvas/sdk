import {
  BaseTransformer,
  TransformerInputEvent,
  TransformType,
} from "../core/types";
import { MessageBroker } from "../messages/message.broker";

export type TableRegistration = {
  schemaName: string;
};

export class TransformerExecutor extends BaseTransformer {
  transform: TransformType;

  messageBroker: MessageBroker;

  transformType = "transform" as const;

  constructor(
    messageBroker: MessageBroker,
    uniqueId: string,
    transformer: TransformType
  ) {
    super(uniqueId);
    this.uniqueId = uniqueId;
    this.transform = transformer;
    this.messageBroker = messageBroker;
    messageBroker.subscribeToTableUpdated((event) => {
      // todo: check if we should subscribe to this table
      this.executeFull();
    });
  }

  async execute(event: TransformerInputEvent): Promise<void> {
    try {
      switch (event.type) {
        case "records":
        case "run":
          await this.executeFull();
      }
    } catch (e) {
      console.error(`Error executing transformer: ${e}`);
    }
  }

  async executeFull(): Promise<void> {
    if (this.transform.type === "sql") {
      const tryTransform = this.transform();
      const { query, tableName, primaryKeys, schemaName } = tryTransform;
      this.messageBroker.publishNewWrites({
        type: "sql_table",
        query,
        tableName,
        primaryKeys,
        schemaName,
      });
    }
  }
}
