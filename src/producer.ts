import { inspect } from "util"
import { Connection } from "./connection"
import { PublishRequest, PublishableMessage } from "./requests/publish_request"
import { PublishConfirmResponse } from "./responses/publish_confirm_response"
import { PublishErrorResponse } from "./responses/publish_error_response"

export type MessageApplicationProperties = Record<string, string | number>

export type MessageAnnotations = Record<string, string | number>

const schedule = typeof setImmediate === "function" ? setImmediate : process.nextTick
//const schedule = process.nextTick

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
  private messageQueue: PublishableMessage[]
  private chunkSize: number
  private sendDuration: number
  private maxBufferLength: number

  constructor(params: {
    connection: Connection
    stream: string
    publisherId: number
    publisherRef?: string
    boot?: boolean
    chunkSize?: number
    sendDuration?: number
    maxBufferLength?: number
  }) {
    this.connection = params.connection
    this.stream = params.stream
    this.publisherId = params.publisherId
    this.publisherRef = params.publisherRef || ""
    this.boot = params.boot || false
    this.publishingId = params.boot ? -1n : 0n
    this.messageQueue = []
    this.chunkSize = params.chunkSize || 100
    this.sendDuration = params.sendDuration || 10
    this.maxBufferLength = params.maxBufferLength || 100
    this.scheduleSendChunks()
  }

  send(args0: bigint | Buffer, arg1: Buffer | MessageOptions = {}, opts: MessageOptions = {}) {
    if (Buffer.isBuffer(args0) && !Buffer.isBuffer(arg1)) {
      return this.enqueueWithPublisherSequence(args0, arg1)
    }

    if (typeof args0 !== "bigint" || !Buffer.isBuffer(arg1)) {
      throw new Error("Message should be a Buffer")
    }

    const msg = { publishingId: args0, message: { content: arg1, ...opts } }

    return this.enqueue(msg)
  }

  public on(_eventName: "publish_confirm", cb: PublishConfirmCallback) {
    this.connection.on("publish_confirm", (confirm: PublishConfirmResponse) => cb(null, confirm.publishingIds))
    this.connection.on("publish_error", (error: PublishErrorResponse) =>
      cb(error.publishingError.code, [error.publishingError.publishingId])
    )
  }

  private async enqueueWithPublisherSequence(message: Buffer, opts: MessageOptions) {
    if (this.boot && this.publishingId === -1n) {
      this.publishingId = await this.getLastPublishingId()
    }
    this.publishingId = this.publishingId + 1n

    const msg = {
      publishingId: this.publishingId,
      message: {
        content: message,
        messageProperties: opts.messageProperties,
        applicationProperties: opts.applicationProperties,
        messageAnnotations: opts.messageAnnotations,
      },
    }

    return this.enqueue(msg)
  }

  getLastPublishingId(): Promise<bigint> {
    return this.connection.queryPublisherSequence({ stream: this.stream, publisherRef: this.publisherRef })
  }

  get ref() {
    return this.publisherRef
  }

  private async enqueue(msg: PublishableMessage) {
    this.messageQueue.push(msg)

    if (this.messageQueue.length > this.maxBufferLength) {
      //if (Math.random() > 0.9999) console.log(`in NOT scheduled send chunks`)
      await this.sendChunks(false)
    }
  }

  private scheduleSendChunks() {
    schedule(() => {
      //if (Math.random() > 0.9999) console.log(`in scheduled send chunks`)
      this.sendChunks()
        .then((v) => v)
        .catch((err) => {
          console.log(`in send callback err ${inspect(err)}`)
        })
    })
  }

  private async sendChunks(scheduleNext = true) {
    const dtStart = Date.now()
    let delta = 0
    while (this.messageQueue.length > 0 && delta < this.sendDuration) {
      await this.sendChunk()
      delta = Date.now() - dtStart
    }
    if (scheduleNext) {
      this.scheduleSendChunks()
    }
  }

  private async sendChunk() {
    if (this.messageQueue.length > 0) {
      const chunk = this.messageQueue.slice(0, this.chunkSize)
      this.messageQueue.splice(0, this.chunkSize)

      await this.connection.send(
        new PublishRequest({
          publisherId: this.publisherId,
          messages: chunk,
        })
      )

      return chunk.length
    }
    return 0
  }

  public getBufferSize() {
    return this.messageQueue.length
  }
}
