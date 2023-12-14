import { inspect } from "util"
import { messageSize } from "./amqp10/encoder"
import { CompressionType } from "./compression"
import { Connection } from "./connection"
import { Logger } from "./logger"
import { FrameSizeException } from "./requests/frame_size_exception"
import { PublishRequest, PublishRequestMessage } from "./requests/publish_request"
import { SubEntryBatchPublishRequest } from "./requests/sub_entry_batch_publish_request"
import { PublishConfirmResponse } from "./responses/publish_confirm_response"
import { PublishErrorResponse } from "./responses/publish_error_response"
import { DEFAULT_UNLIMITED_FRAME_MAX } from "./util"

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
  send(message: Buffer, opts?: MessageOptions): Promise<boolean>
  basicSend(publishingId: bigint, content: Buffer, opts?: MessageOptions): Promise<boolean>
  flush(): Promise<boolean>
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
  private maxFrameSize: number
  private queue: PublishRequestMessage[]
  private scheduled: NodeJS.Immediate | null
  private logger: Logger
  private maxChunkLength: number

  constructor(params: {
    connection: Connection
    stream: string
    publisherId: number
    publisherRef?: string
    boot?: boolean
    maxFrameSize: number
    maxChunkLength?: number
    logger: Logger
  }) {
    this.connection = params.connection
    this.stream = params.stream
    this.publisherId = params.publisherId
    this.publisherRef = params.publisherRef || ""
    this.boot = params.boot || false
    this.publishingId = params.boot ? -1n : 0n
    this.maxFrameSize = params.maxFrameSize || 1048576
    this.queue = []
    this.scheduled = null
    this.logger = params.logger
    this.maxChunkLength = params.maxChunkLength || 100
  }

  async send(message: Buffer, opts: MessageOptions = {}) {
    if (this.boot && this.publishingId === -1n) {
      this.publishingId = await this.getLastPublishingId()
    }
    this.publishingId = this.publishingId + 1n

    return this.basicSend(this.publishingId, message, opts)
  }

  basicSend(publishingId: bigint, content: Buffer, opts: MessageOptions = {}) {
    const msg = { publishingId: publishingId, message: { content: content, ...opts } }
    return this.enqueue(msg)
  }

  async flush() {
    await this.sendChunk()
    return true
  }

  async sendSubEntries(messages: Message[], compressionType: CompressionType = CompressionType.None) {
    return this.connection.send(
      new SubEntryBatchPublishRequest({
        publisherId: this.publisherId,
        publishingId: this.publishingId,
        compression: this.connection.getCompression(compressionType),
        maxFrameSize: this.maxFrameSize,
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

  private async enqueue(publishRequestMessage: PublishRequestMessage) {
    this.checkMessageSize(publishRequestMessage)
    const sendCycleNeeded = this.add(publishRequestMessage)
    let sent = false
    if (sendCycleNeeded) {
      await this.sendChunk()
      sent = true
    }
    this.scheduleIfNeeded()

    return sent
  }

  private checkMessageSize(publishRequestMessage: PublishRequestMessage) {
    const computedSize = messageSize(publishRequestMessage.message)
    if (this.maxFrameSize !== DEFAULT_UNLIMITED_FRAME_MAX && computedSize > this.maxFrameSize) {
      throw new FrameSizeException(`Message too big to fit in one frame: ${computedSize}`)
    }

    return true
  }

  private async sendChunk() {
    const chunk = this.popChunk()
    if (chunk.length > 0) {
      await this.connection.send(
        new PublishRequest({
          publisherId: this.publisherId,
          messages: chunk,
        })
      )
    }
  }

  private scheduleIfNeeded() {
    if (this.queue.length > 0 && this.scheduled === null) {
      this.scheduled = setImmediate(() => {
        this.scheduled = null
        this.flush()
          .then((_v) => _v)
          .catch((err) => this.logger.error(`Error in send: ${inspect(err)}`))
          .finally(() => this.scheduleIfNeeded())
      })
    }
  }

  private add(message: PublishRequestMessage) {
    this.queue.push(message)

    return this.queue.length >= this.maxChunkLength
  }

  private popChunk() {
    return this.queue.splice(0, this.maxChunkLength)
  }
}
