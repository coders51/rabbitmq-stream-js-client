import { CompressionType } from "./compression"
import { Connection } from "./connection"
import { PublishRequest } from "./requests/publish_request"
import { SubEntryBatchPublishRequest } from "./requests/sub_entry_batch_publish_request"
import { PublishConfirmResponse } from "./responses/publish_confirm_response"
import { PublishErrorResponse } from "./responses/publish_error_response"

export type MessageApplicationProperties = Record<string, string | number>

export type MessageAnnotations = Record<string, string | number>

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

export interface MessageHeader {
  durable?: boolean
  priority?: number
  ttl?: number
  firstAcquirer?: boolean
  deliveryCount?: number
}

export interface Message {
  content: Buffer
  messageProperties?: MessageProperties
  messageHeader?: MessageHeader
  applicationProperties?: MessageApplicationProperties
  messageAnnotations?: MessageAnnotations
  amqpValue?: string
  offset?: bigint
}

interface MessageOptions {
  messageProperties?: MessageProperties
  applicationProperties?: Record<string, string | number>
  messageAnnotations?: Record<string, string | number>
}
type PublishConfirmCallback = (err: number | null, publishingIds: bigint[]) => void
export class Producer {
  private connection: Connection
  private stream: string
  readonly publisherId: number
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

  public on(_eventName: "publish_confirm", cb: PublishConfirmCallback) {
    this.connection.on("publish_confirm", (confirm: PublishConfirmResponse) => cb(null, confirm.publishingIds))
    this.connection.on("publish_error", (error: PublishErrorResponse) =>
      cb(error.publishingError.code, [error.publishingError.publishingId])
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
        messages: [
          {
            publishingId: this.publishingId,
            message: {
              content: message,
              messageProperties: opts.messageProperties,
              applicationProperties: opts.applicationProperties,
              messageAnnotations: opts.messageAnnotations,
            },
          },
        ],
      })
    )
  }

  async sendSubEntries(messages: Message[], compressionType: CompressionType = CompressionType.None) {
    const compression = this.connection.getCompression(compressionType)

    return this.connection.send(
      new SubEntryBatchPublishRequest({
        publisherId: this.publisherId,
        publishingId: this.publishingId,
        compression: compression,
        messages: messages,
      })
    )
  }

  getLastPublishingId(): Promise<bigint> {
    return this.connection.queryPublisherSequence({ stream: this.stream, publisherRef: this.publisherRef })
  }

  get ref() {
    return this.publisherRef
  }
}
