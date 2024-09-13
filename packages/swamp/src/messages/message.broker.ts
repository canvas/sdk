import { TableUpdatedEvent, WriteRecordsEvent } from "../core/types";

export type HandleWriteRecords = (event: WriteRecordsEvent) => void;
export type HandleTableUpdated = (event: TableUpdatedEvent) => void;

export interface MessageBroker {
  publishNewWrites(event: WriteRecordsEvent): void;
  subscribeToNewWrites(subscriber: HandleWriteRecords): void;
  publishTableUpdated(event: TableUpdatedEvent): void;
  subscribeToTableUpdated(subscriber: HandleTableUpdated): void;
}
