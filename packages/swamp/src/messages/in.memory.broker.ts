import { CreateRecordsEvent, DataLocation } from "../core/types";
import {
  MessageBroker,
  HandleCreateRecords,
  HandleNewDataLocation,
} from "./message.broker";

export class InMemoryBroker implements MessageBroker {
  createRecordsSubscribers: HandleCreateRecords[] = [];
  mewDataLocationSubscribers: HandleNewDataLocation[] = [];

  publishNewRecords(event: CreateRecordsEvent): void {
    for (const subscriber of this.createRecordsSubscribers) {
      subscriber(event);
    }
  }

  subscribeToNewRecords(subscriber: HandleCreateRecords): void {
    this.createRecordsSubscribers.push(subscriber);
  }

  publishNewDataLocation(event: DataLocation): void {
    for (const subscriber of this.mewDataLocationSubscribers) {
      subscriber(event);
    }
  }
  subscribeToNewDataLocation(subscriber: HandleNewDataLocation): void {
    this.mewDataLocationSubscribers.push(subscriber);
  }
}
