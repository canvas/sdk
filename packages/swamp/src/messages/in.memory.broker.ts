import { InsertRecordsEvent, TableUpdatedEvent } from "../core/types";
import {
  MessageBroker,
  HandleWriteRecords,
  HandleTableUpdated,
} from "./message.broker";

export class InMemoryBroker implements MessageBroker {
  createRecordsSubscribers: HandleWriteRecords[] = [];
  tableUpdatedSubscribers: HandleTableUpdated[] = [];
  publishNewWrites(event: InsertRecordsEvent): void {
    for (const subscriber of this.createRecordsSubscribers) {
      subscriber(event);
    }
  }

  subscribeToNewWrites(subscriber: HandleWriteRecords): void {
    this.createRecordsSubscribers.push(subscriber);
  }

  publishTableUpdated(event: TableUpdatedEvent): void {
    for (const subscriber of this.tableUpdatedSubscribers) {
      subscriber(event);
    }
  }

  subscribeToTableUpdated(subscriber: HandleTableUpdated): void {
    this.tableUpdatedSubscribers.push(subscriber);
  }
}
