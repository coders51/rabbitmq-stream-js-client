import { inspect } from "util"
import { messageSize } from "./amqp10/encoder"
import { CompressionType } from "./compression"
import { Connection, ConnectionInfo } from "./connection"
import { ConnectionPool } from "./connection_pool"
import { Logger } from "./logger"
import { FrameSizeException } from "./requests/frame_size_exception"
import { PublishRequest, PublishRequestMessage } from "./requests/publish_request"
import { PublishRequestV2 } from "./requests/publish_request_v2"
import { SubEntryBatchPublishRequest } from "./requests/sub_entry_batch_publish_request"
import { PublishConfirmResponse } from "./responses/publish_confirm_response"
import { PublishErrorResponse } from "./responses/publish_error_response"
import { DEFAULT_UNLIMITED_FRAME_MAX } from "./util"
import { MetadataUpdateListener } from "./response_decoder"

export type MessageApplicationProperties = Record<string, string | number>
export type MessageAnnotations = Record<string, MessageAnnotationsValue>
export type MessageAnnotationsValue = string | number | AmqpByte

export class AmqpByte {
  private value: number

  constructor(value: number) {
    if (value > 255 || value < 0) {
      throw new Error("Invalid byte, value must be between 0 and 255")
    }
    this.value = value
  }

  public get byteValue() {
    return this.value
  }
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
  messageAnnotations?: Record<string, MessageAnnotationsValue>
  publishingId?: bigint
}

export const computeExtendedPublisherId = (publisherId: number, connectionId: string) => {
  return `${publisherId}@${connectionId}`
}

export interface Publisher {
  /**
   * Sends a message in the stream
   *
   * @param {Buffer} message - The encoded content of the message
   * @param {MessageOptions} opts - The optional message options and properties
   * @returns {SendResult} Returns a boolean value and the associated publishingId
   */
  send(message: Buffer, opts?: MessageOptions): Promise<SendResult>

  /**
   * Sends a message in the stream with a specific publishingId
   *
   * @param {bigint} publishingId - The associated publishingId
   * @param {Buffer} content - The encoded content of the message
   * @param {MessageOptions} opts - The optional message options and properties
   * @returns {SendResult} Returns a boolean value and the associated publishingId
   */
  basicSend(publishingId: bigint, content: Buffer, opts?: MessageOptions): Promise<SendResult>

  /**
   * Sends all the accumulated messages on the internal buffer
   *
   * @returns {boolean} Returns false if there was an error
   */
  flush(): Promise<boolean>

  /**
   * Sends a batch of messages
   *
   * @param {Message[]} messages - A batch of messages to send
   * @param {CompressionType} compressionType - Can optionally compress the messages
   */
  sendSubEntries(messages: Message[], compressionType?: CompressionType): Promise<void>

  /**
   * Setup the listener for the metadata update event
   *
   * @param {"metadata_update"} event - The name of the event
   * @param {MetadataUpdateListener} listener - The listener which will be called when the event is fired
   */
  on(event: "metadata_update", listener: MetadataUpdateListener): void

  /**
   * Setup the listener for the publish confirm event
   *
   * @param {"publish_confirm"} event - The name of the event
   * @param {PublishConfirmCallback} listener - The listener which will be called when the event is fired
   */
  on(event: "publish_confirm", listener: PublishConfirmCallback): void

  /**
   * Gets the last publishing id in the stream
   *
   * @returns {bigint} Last publishing id
   */
  getLastPublishingId(): Promise<bigint>

  /**
   * Gets the infos of the publisher's connection
   *
   * @returns {ConnectionInfo} Infos on the publisher's connection
   */
  getConnectionInfo(): ConnectionInfo

  /**
   * Close the publisher
   */
  close(): Promise<void>

  closed: boolean
  ref: string
  readonly publisherId: number
  readonly extendedId: string
}

export type FilterFunc = (msg: Message) => string | undefined
type PublishConfirmCallback = (err: number | null, publishingIds: bigint[]) => void
export type SendResult = { sent: boolean; publishingId: bigint }
export class StreamPublisher implements Publisher {
  private connection: Connection
  private stream: string
  readonly publisherId: number
  protected publisherRef: string
  private publishingId: bigint
  private maxFrameSize: number
  private queue: PublishRequestMessage[]
  private scheduled: NodeJS.Immediate | null
  private logger: Logger
  private maxChunkLength: number
  private _closed = false

  constructor(
    private pool: ConnectionPool,
    params: {
      connection: Connection
      stream: string
      publisherId: number
      publisherRef?: string
      maxFrameSize: number
      maxChunkLength?: number
      logger: Logger
    },
    publishingId: bigint,
    private readonly filter?: FilterFunc
  ) {
    this.connection = params.connection
    this.stream = params.stream
    this.publisherId = params.publisherId
    this.publisherRef = params.publisherRef || ""
    this.publishingId = publishingId
    this.maxFrameSize = params.maxFrameSize || 1048576
    this.queue = []
    this.scheduled = null
    this.logger = params.logger
    this.maxChunkLength = params.maxChunkLength || 100
    this.connection.incrRefCount()
  }

  public get closed(): boolean {
    return this._closed
  }

  async send(message: Buffer, opts: MessageOptions = {}): Promise<SendResult> {
    if (this._closed) {
      throw new Error(`Publisher has been closed`)
    }

    let publishingIdToSend: bigint
    if (this.publisherRef && opts.publishingId) {
      publishingIdToSend = opts.publishingId
      if (opts.publishingId > this.publishingId) {
        this.publishingId = opts.publishingId
      }
    } else {
      this.publishingId = this.publishingId + 1n
      publishingIdToSend = this.publishingId
    }

    return await this.basicSend(publishingIdToSend, message, opts)
  }

  async basicSend(publishingId: bigint, content: Buffer, opts: MessageOptions = {}): Promise<SendResult> {
    const msg = { publishingId: publishingId, message: { content: content, ...opts } }
    return await this.enqueue(msg)
  }

  async flush() {
    return await this.sendBuffer()
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
    const { host, port, id, writable, localPort, ready, vhost } = this.connection.getConnectionInfo()
    return { host, port, id, writable, localPort, ready, vhost }
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
      await this.pool.releaseConnection(this.connection, true)
    }
    this._closed = true
  }

  public async automaticClose(): Promise<void> {
    if (!this.closed) {
      await this.flush()
      await this.pool.releaseConnection(this.connection, false)
    }
    this._closed = true
  }

  public get streamName(): string {
    return this.stream
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
    const result = { sent: false, publishingId: publishRequestMessage.publishingId }
    if (sendCycleNeeded) {
      result.sent = await this.sendBuffer()
    }
    this.scheduleIfNeeded()
    return result
  }

  private checkMessageSize(publishRequestMessage: PublishRequestMessage) {
    const computedSize = messageSize(publishRequestMessage.message)
    if (this.maxFrameSize !== DEFAULT_UNLIMITED_FRAME_MAX && computedSize > this.maxFrameSize) {
      throw new FrameSizeException(`Message too big to fit in one frame: ${computedSize}`)
    }

    return true
  }

  private async sendBuffer() {
    if (!this.connection.ready) {
      return false
    }
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
      return true
    }
    return false
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

  public get extendedId(): string {
    return computeExtendedPublisherId(this.publisherId, this.connection.connectionId)
  }
}
