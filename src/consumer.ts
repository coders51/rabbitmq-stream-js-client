import { ConsumerFilter } from "./client"
import { ConnectionInfo, Connection } from "./connection"
import { ConnectionPool } from "./connection_pool"
import { ConsumerCreditPolicy, defaultCreditPolicy } from "./consumer_credit_policy"
import { Message } from "./publisher"
import { Offset } from "./requests/subscribe_request"

export type ConsumerFunc = (message: Message, consumer: StreamConsumer) => Promise<void> | void
export type ConsumerUpdateListener = (consumerRef: string, streamName: string) => Promise<Offset>
export const computeExtendedConsumerId = (consumerId: number, connectionId: string) => {
  return `${consumerId}@${connectionId}`
}

export interface Consumer {
  /**
   * Close the publisher
   */
  close(): Promise<void>

  /**
   * Store the stream offset on the server
   *
   * @param {bigint} offsetValue - The value of the offset to save, if not specified the local offset is used
   */
  storeOffset(offsetValue?: bigint): Promise<void>

  /**
   * Get the saved offset on the server
   *
   * @returns {bigint} The value of the stream offset
   */
  queryOffset(): Promise<bigint>

  /**
   * Get the stream local offset
   */
  getOffset(): bigint

  /**
   * Gets the infos of the publisher's connection
   *
   * @returns {ConnectionInfo} Infos on the publisher's connection
   */
  getConnectionInfo(): ConnectionInfo

  /**
   * Updates the offset of the consumer instance
   *
   * @param {Offset} offset - The new offset to set
   */
  updateConsumerOffset(offset: Offset): void

  consumerId: number
  consumerRef?: string
  readonly extendedId: string
}

export class StreamConsumer implements Consumer {
  private connection: Connection
  private stream: string
  public consumerId: number
  public consumerRef?: string
  public consumerTag?: string
  public offset: Offset
  public consumerUpdateListener?: ConsumerUpdateListener
  private clientLocalOffset: Offset
  private creditsHandler: ConsumerCreditPolicy
  private consumerHandle: ConsumerFunc
  private closed: boolean
  private singleActive: boolean = false

  constructor(
    private pool: ConnectionPool,
    handle: ConsumerFunc,
    params: {
      connection: Connection
      stream: string
      consumerId: number
      consumerRef?: string
      consumerTag?: string
      offset: Offset
      creditPolicy?: ConsumerCreditPolicy
      singleActive?: boolean
      consumerUpdateListener?: ConsumerUpdateListener
    },
    readonly filter?: ConsumerFilter
  ) {
    this.connection = params.connection
    this.stream = params.stream
    this.consumerId = params.consumerId
    this.consumerRef = params.consumerRef
    this.offset = params.offset
    this.clientLocalOffset = this.offset.clone()
    this.connection.incrRefCount()
    this.creditsHandler = params.creditPolicy || defaultCreditPolicy
    this.consumerHandle = handle
    this.consumerUpdateListener = params.consumerUpdateListener
    this.closed = false
    this.singleActive = params.singleActive ?? false
  }

  async close(): Promise<void> {
    this.closed = true
    await this.pool.releaseConnection(this.connection, true)
  }

  async automaticClose(): Promise<void> {
    this.closed = true
    await this.pool.releaseConnection(this.connection, false)
  }

  public storeOffset(offsetValue?: bigint): Promise<void> {
    if (!this.consumerRef) throw new Error("ConsumerReference must be defined in order to use this!")
    const offset = offsetValue ? offsetValue : this.clientLocalOffset.value ?? 0n
    return this.connection.storeOffset({ stream: this.stream, reference: this.consumerRef, offsetValue: offset })
  }

  public queryOffset(): Promise<bigint> {
    if (!this.consumerRef) throw new Error("ConsumerReference must be defined in order to use this!")
    return this.connection.queryOffset({ stream: this.stream, reference: this.consumerRef })
  }

  getOffset(): bigint {
    return this.clientLocalOffset.value ?? 0n
  }

  public getConnectionInfo(): ConnectionInfo {
    const { host, port, id, readable, localPort, ready, vhost } = this.connection.getConnectionInfo()
    return { host, port, id, readable, localPort, ready, vhost }
  }

  public async handle(message: Message) {
    if (this.closed || this.isMessageOffsetLessThanConsumers(message)) return
    await this.consumerHandle(message, this)
    this.maybeUpdateLocalOffset(message)
  }

  public get streamName(): string {
    return this.stream
  }

  public get extendedId(): string {
    return computeExtendedConsumerId(this.consumerId, this.connection.connectionId)
  }

  public get creditPolicy() {
    return this.creditsHandler
  }

  public get isSingleActive() {
    return this.singleActive
  }

  public updateConsumerOffset(offset: Offset) {
    this.offset = offset.clone()
    this.clientLocalOffset = offset.clone()
  }

  private maybeUpdateLocalOffset(message: Message) {
    if (message.offset !== undefined) this.clientLocalOffset = Offset.offset(message.offset)
  }

  // TODO -- Find better name?
  private isMessageOffsetLessThanConsumers(message: Message) {
    return this.offset.type === "numeric" && message.offset !== undefined && message.offset < this.offset.value!
  }
}
