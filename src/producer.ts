import { Connection } from "./connection"
import { PublishRequest } from "./requests/publish_request"

export class Producer {
  constructor(private connection: Connection, private publisherId: number) {}

  send(publishingId: bigint, message: Buffer) {
    return this.connection.send(
      new PublishRequest({ publisherId: this.publisherId, messages: [{ publishingId, message }] })
    )
  }
}
