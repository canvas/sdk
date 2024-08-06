import { CreateRecordsEvent } from '../core/types';

export type HandleCreateRecords = (event: CreateRecordsEvent) => void;

export interface MessageBroker {
  publishNewRecords(event: CreateRecordsEvent): void;
  subscribeToNewRecords(subscriber: HandleCreateRecords): void;
}