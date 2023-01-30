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
  private boot: boolean
  private publishingId: bigint

  constructor(params: {
    connection: Connection
    stream: string
    publisherId: number
    publisherRef: string
    boot?: boolean
  }) {
    this.connection = params.connection
    this.stream = params.stream
    this.publisherId = params.publisherId
    this.publisherRef = params.publisherRef
    this.boot = params.boot || false
    this.publishingId = params.boot ? -1n : 0n
  }

  /*
  @deprecate This method should not be used
  */
  send(publishingId: bigint, message: Buffer): Promise<void>
  send(message: Buffer): Promise<void>

  send(args0: bigint | Buffer, message?: Buffer, opts: { properties?: MessageProperties } = {}) {
    if (Buffer.isBuffer(args0)) {
      return this.sendWithPublisherSequence(args0, opts)
    }

    if (!Buffer.isBuffer(message)) {
      throw new Error("Message should be a Buffer")
    }

    return this.connection.send(
      new PublishRequest({
        publisherId: this.publisherId,
        messages: [{ publishingId: args0, message: { content: message, properties: opts.properties } }],
      })
    )
  }

  private async sendWithPublisherSequence(message: Buffer, opts: { properties?: MessageProperties } = {}) {
    if (this.boot && this.publishingId === -1n) {
      this.publishingId = await this.getLastPublishingId()
    }
    this.publishingId = this.publishingId + 1n

    return this.connection.send(
      new PublishRequest({
        publisherId: this.publisherId,
        messages: [{ publishingId: this.publishingId, message: { content: message, properties: opts.properties } }],
      })
    )
  }

  getLastPublishingId() {
    return this.connection.queryPublisherSequence({ stream: this.stream, publisherRef: this.publisherRef })
  }
}
