import { EventEmitter } from "stream"
import { Connection } from "./connection"
import { PublishRequest } from "./requests/publish_request"
import { PublishConfirmResponse } from "./responses/publish_confirm_response"

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
  messageProperties?: MessageProperties
  applicationProperties?: MessageApplicationProperties
}

interface MessageOptions {
  messageProperties?: MessageProperties
  applicationProperties?: Record<string, string | number>
}
type PublishConfirmCallback = (err: Error | null, publishingIds: bigint[]) => void
type PublisherEvent = "publish_confirm"

export class Producer {
  private connection: Connection
  private stream: string
  private publisherId: number
  private publisherRef: string
  private boot: boolean
  private publishingId: bigint
  private eventEmitter: EventEmitter

  constructor(params: {
    connection: Connection
    stream: string
    publisherId: number
    publisherRef?: string
    boot?: boolean
    emitter: EventEmitter
  }) {
    this.connection = params.connection
    this.stream = params.stream
    this.publisherId = params.publisherId
    this.publisherRef = params.publisherRef || ""
    this.boot = params.boot || false
    this.publishingId = params.boot ? -1n : 0n
    this.eventEmitter = params.emitter
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

  public on(eventName: PublisherEvent, cb: PublishConfirmCallback) {
    switch (eventName) {
      case "publish_confirm":
        this.eventEmitter.removeAllListeners("publish_confirm")
        this.eventEmitter.on("publish_confirm", (confirm: PublishConfirmResponse) => cb(null, confirm.publishingIds))
        break
      default:
        throw Error(`Event ${eventName} was not recognized`)
    }
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
            },
          },
        ],
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
