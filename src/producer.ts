import { Connection } from "./connection"
import { PublishRequest } from "./requests/publish_request"

export class Producer {
  private connection: Connection
  private stream: string
  private publisherId: number
  private publisherRef: string
  constructor(params: { connection: Connection; stream: string; publisherId: number; publisherRef: string }) {
    this.connection = params.connection
    this.stream = params.stream
    this.publisherId = params.publisherId
    this.publisherRef = params.publisherRef
  }

  send(publishingId: bigint, message: Buffer) {
    return this.connection.send(
      new PublishRequest({ publisherId: this.publisherId, messages: [{ publishingId, message }] })
    )
  }

  getLastPublishingId() {
    return this.connection.queryPublisherSequence({ stream: this.stream, publisherRef: this.publisherRef })
  }
}
