import { inspect } from "util"
import { messageSize } from "./amqp10/encoder"
import { Client } from "./client"
import { CompressionType } from "./compression"
import { Logger } from "./logger"
import { FrameSizeException } from "./requests/frame_size_exception"
import { PublishRequest, PublishRequestMessage } from "./requests/publish_request"
import { SubEntryBatchPublishRequest } from "./requests/sub_entry_batch_publish_request"
import { PublishConfirmResponse } from "./responses/publish_confirm_response"
import { PublishErrorResponse } from "./responses/publish_error_response"
import { DEFAULT_UNLIMITED_FRAME_MAX } from "./util"
import { MetadataUpdateListener } from "./response_decoder"

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
  on(event: "metadata_update", listener: MetadataUpdateListener): void
  on(event: "publish_confirm", listener: PublishConfirmCallback): void
  getLastPublishingId(): Promise<bigint>
  getConnectionInfo(): { host: string; port: number; id: string }
  close(): Promise<void>
  ref: string
  readonly publisherId: number
}

type PublishConfirmCallback = (err: number | null, publishingIds: bigint[]) => void
export class StreamProducer implements Producer {
  private client: Client
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
    client: Client
    stream: string
    publisherId: number
    publisherRef?: string
    boot?: boolean
    maxFrameSize: number
    maxChunkLength?: number
    logger: Logger
  }) {
    this.client = params.client
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
    await this.sendBuffer()
    return true
  }

  async sendSubEntries(messages: Message[], compressionType: CompressionType = CompressionType.None) {
    return this.client.send(
      new SubEntryBatchPublishRequest({
        publisherId: this.publisherId,
        publishingId: this.publishingId,
        compression: this.client.getCompression(compressionType),
        maxFrameSize: this.maxFrameSize,
        messages: messages,
      })
    )
  }

  public getConnectionInfo(): { host: string; port: number; id: string } {
    return this.client.getConnectionInfo()
  }

  public on(event: "metadata_update", listener: MetadataUpdateListener): void
  public on(event: "publish_confirm", listener: PublishConfirmCallback): void
  public on(
    event: "metadata_update" | "publish_confirm",
    listener: MetadataUpdateListener | PublishConfirmCallback
  ): void {
    switch (event) {
      case "metadata_update":
        this.client.on("metadata_update", listener as MetadataUpdateListener)
        break
      case "publish_confirm":
        const cb = listener as PublishConfirmCallback
        this.client.on("publish_confirm", (confirm: PublishConfirmResponse) => cb(null, confirm.publishingIds))
        this.client.on("publish_error", (error: PublishErrorResponse) =>
          cb(error.publishingError.code, [error.publishingError.publishingId])
        )
        break
      default:
        break
    }
  }

  getLastPublishingId(): Promise<bigint> {
    return this.client.queryPublisherSequence({ stream: this.stream, publisherRef: this.publisherRef })
  }

  get ref() {
    return this.publisherRef
  }

  public async close(): Promise<void> {
    await this.flush()
    await this.client.close()
  }

  private async enqueue(publishRequestMessage: PublishRequestMessage) {
    this.checkMessageSize(publishRequestMessage)
    const sendCycleNeeded = this.add(publishRequestMessage)
    let sent = false
    if (sendCycleNeeded) {
      await this.sendBuffer()
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

  private async sendBuffer() {
    const chunk = this.popChunk()
    if (chunk.length > 0) {
      await this.client.send(
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
