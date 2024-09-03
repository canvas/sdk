import { CreateRecordsEvent } from "../core/types";
import { MessageBroker, HandleCreateRecords } from "./message.broker";

export class InMemoryBroker implements MessageBroker {
  createRecordsSubscribers: HandleCreateRecords[] = [];

  publishNewRecords(event: CreateRecordsEvent): void {
    console.log(
      `Publish to ${this.createRecordsSubscribers.length} subscribers`
    );
    for (const subscriber of this.createRecordsSubscribers) {
      subscriber(event);
    }
  }

  subscribeToNewRecords(subscriber: HandleCreateRecords): void {
    this.createRecordsSubscribers.push(subscriber);
  }
}
