import { Connection } from "./connection"
import { PublishRequest } from "./requests/publish_request"

export interface Message {
  content: Buffer
  properties?: MessageProperties
}

export interface MessageProperties {
  contentType?: string
  contentEncoding?: string
  replyTo?: string
  to?: string
  subject?: string
  correlationId?: string
  messageId?: string
  userId?: Buffer
  absoluteExpiryTime?: Date
  creationTime?: Date
  groupId?: string
  groupSequence?: number
  replyToGroupId?: string
}

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

  send(publishingId: bigint, message: Buffer, opts: { properties?: MessageProperties } = {}) {
    return this.connection.send(
      new PublishRequest({
        publisherId: this.publisherId,
        messages: [{ publishingId, message: { content: message, properties: opts.properties } }],
      })
    )
  }

  getLastPublishingId() {
    return this.connection.queryPublisherSequence({ stream: this.stream, publisherRef: this.publisherRef })
  }
}
