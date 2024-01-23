import { inspect } from "util"
import { messageSize } from "./amqp10/encoder"
import { CompressionType } from "./compression"
import { Logger } from "./logger"
import { FrameSizeException } from "./requests/frame_size_exception"
import { PublishRequest, PublishRequestMessage } from "./requests/publish_request"
import { SubEntryBatchPublishRequest } from "./requests/sub_entry_batch_publish_request"
import { PublishConfirmResponse } from "./responses/publish_confirm_response"
import { PublishErrorResponse } from "./responses/publish_error_response"
import { DEFAULT_UNLIMITED_FRAME_MAX, REQUIRED_MANAGEMENT_VERSION } from "./util"
import { MetadataUpdateListener } from "./response_decoder"
import { ConnectionInfo, Connection } from "./connection"
import { ConnectionPool } from "./connection_pool"
import { coerce, lt } from "semver"
import { PublishRequestV2 } from "./requests/publish_request_v2"

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

export interface MessageOptions {
  messageProperties?: MessageProperties
  applicationProperties?: Record<string, string | number>
  messageAnnotations?: Record<string, string | number>
}

export interface Publisher {
  send(message: Buffer, opts?: MessageOptions): Promise<boolean>
  basicSend(publishingId: bigint, content: Buffer, opts?: MessageOptions): Promise<boolean>
  flush(): Promise<boolean>
  sendSubEntries(messages: Message[], compressionType?: CompressionType): Promise<void>
  on(event: "metadata_update", listener: MetadataUpdateListener): void
  on(event: "publish_confirm", listener: PublishConfirmCallback): void
  getLastPublishingId(): Promise<bigint>
  getConnectionInfo(): ConnectionInfo
  close(): Promise<void>
  ref: string
  readonly publisherId: number
}

export type FilterFunc = (msg: Message) => string | undefined
type PublishConfirmCallback = (err: number | null, publishingIds: bigint[]) => void
export class StreamPublisher implements Publisher {
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
  private closed = false

  constructor(
    params: {
      connection: Connection
      stream: string
      publisherId: number
      publisherRef?: string
      boot?: boolean
      maxFrameSize: number
      maxChunkLength?: number
      logger: Logger
    },
    private readonly filter?: FilterFunc
  ) {
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
    this.connection.incrRefCount()
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

  public getConnectionInfo(): ConnectionInfo {
    const { host, port, id, writable, localPort } = this.connection.getConnectionInfo()
    return { host, port, id, writable, localPort }
  }

  public on(event: "metadata_update", listener: MetadataUpdateListener): void
  public on(event: "publish_confirm", listener: PublishConfirmCallback): void
  public on(
    event: "metadata_update" | "publish_confirm",
    listener: MetadataUpdateListener | PublishConfirmCallback
  ): void {
    switch (event) {
      case "metadata_update":
        this.connection.on("metadata_update", listener as MetadataUpdateListener)
        break
      case "publish_confirm":
        const cb = listener as PublishConfirmCallback
        this.connection.on("publish_confirm", (confirm: PublishConfirmResponse) => cb(null, confirm.publishingIds))
        this.connection.on("publish_error", (error: PublishErrorResponse) =>
          cb(error.publishingError.code, [error.publishingError.publishingId])
        )
        break
      default:
        break
    }
  }

  getLastPublishingId(): Promise<bigint> {
    return this.connection.queryPublisherSequence({ stream: this.stream, publisherRef: this.publisherRef })
  }

  get ref() {
    return this.publisherRef
  }

  public async close(): Promise<void> {
    if (!this.closed) {
      await this.flush()
      this.connection.decrRefCount()
      if (ConnectionPool.removeIfUnused(this.connection)) {
        await this.connection.close()
      }
    }
    this.closed = true
  }

  private async enqueue(publishRequestMessage: PublishRequestMessage) {
    if (this.filter) {
      publishRequestMessage.filterValue = this.filter(publishRequestMessage.message)
    }
    if (!this.connection.isFilteringEnabled && this.filter) {
      throw new Error(`Your rabbit server management version does not support filtering.`)
    }
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
      this.filter
        ? await this.connection.send(
            new PublishRequestV2({
              publisherId: this.publisherId,
              messages: chunk,
            })
          )
        : await this.connection.send(
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
