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

export interface Producer {
  send(message: Buffer, opts?: MessageOptions): Promise<void>
  basicSend(publishingId: bigint, content: Buffer, opts?: MessageOptions): Promise<void>
  sendSubEntries(messages: Message[], compressionType?: CompressionType): Promise<void>
  on(eventName: "publish_confirm", cb: PublishConfirmCallback): void
  getLastPublishingId(): Promise<bigint>
  ref: string
  readonly publisherId: number
}

type PublishConfirmCallback = (err: number | null, publishingIds: bigint[]) => void
export class StreamProducer implements Producer {
  private connection: Connection
  private stream: string
  readonly publisherId: number
  protected publisherRef: string
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

  async send(message: Buffer, opts: MessageOptions = {}) {
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

  basicSend(publishingId: bigint, content: Buffer, opts: MessageOptions = {}) {
    return this.connection.send(
      new PublishRequest({
        publisherId: this.publisherId,
        messages: [{ publishingId: publishingId, message: { content: content, ...opts } }],
      })
    )
  }

  async sendSubEntries(messages: Message[], compressionType: CompressionType = CompressionType.None) {
    return this.connection.send(
      new SubEntryBatchPublishRequest({
        publisherId: this.publisherId,
        publishingId: this.publishingId,
        compression: this.connection.getCompression(compressionType),
        messages: messages,
      })
    )
  }

  public on(_eventName: "publish_confirm", cb: PublishConfirmCallback) {
    this.connection.on("publish_confirm", (confirm: PublishConfirmResponse) => cb(null, confirm.publishingIds))
    this.connection.on("publish_error", (error: PublishErrorResponse) =>
      cb(error.publishingError.code, [error.publishingError.publishingId])
    )
  }

  getLastPublishingId(): Promise<bigint> {
    return this.connection.queryPublisherSequence({ stream: this.stream, publisherRef: this.publisherRef })
  }

  get ref() {
    return this.publisherRef
  }
}
