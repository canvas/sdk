import { CreateRecordsEvent, DataLocation } from "../core/types";

export type HandleCreateRecords = (event: CreateRecordsEvent) => void;
export type HandleNewDataLocation = (event: DataLocation) => void;

export interface MessageBroker {
  publishNewRecords(event: CreateRecordsEvent): void;
  subscribeToNewRecords(subscriber: HandleCreateRecords): void;
  publishNewDataLocation(event: DataLocation): void;
  subscribeToNewDataLocation(subscriber: HandleNewDataLocation): void;
}
