import { Connection } from "./connection"
import { PublishRequest } from "./requests/publish_request"

export type MessageApplicationProperties = Record<string, string | number>

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

export interface Message {
  content: Buffer
  properties?: MessageProperties
  applicationProperties?: MessageApplicationProperties
}

interface MessageOptions {
  properties?: MessageProperties
  applicationProperties?: Record<string, string | number>
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
    publisherRef?: string
    boot?: boolean
  }) {
    this.connection = params.connection
    this.stream = params.stream
    this.publisherId = params.publisherId
    this.publisherRef = params.publisherRef || ""
    this.boot = params.boot || false
    this.publishingId = params.boot ? -1n : 0n
  }

  /*
    @deprecate This method should not be used
  */
  send(publishingId: bigint, message: Buffer, opts?: MessageOptions): Promise<void>
  send(message: Buffer, opts?: MessageOptions): Promise<void>

  send(args0: bigint | Buffer, arg1: Buffer | MessageOptions = {}, opts: MessageOptions = {}) {
    if (Buffer.isBuffer(args0) && !Buffer.isBuffer(arg1)) {
      return this.sendWithPublisherSequence(args0, arg1)
    }

    if (typeof args0 !== "bigint" || !Buffer.isBuffer(arg1)) {
      throw new Error("Message should be a Buffer")
    }

    return this.connection.send(
      new PublishRequest({
        publisherId: this.publisherId,
        messages: [{ publishingId: args0, message: { content: arg1, ...opts } }],
      })
    )
  }

  private async sendWithPublisherSequence(message: Buffer, opts: MessageOptions) {
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
