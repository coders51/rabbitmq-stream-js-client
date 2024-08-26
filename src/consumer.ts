import { ConsumerFilter } from "./client"
import { ConnectionInfo, Connection } from "./connection"
import { ConnectionPool } from "./connection_pool"
import { ConsumerCreditPolicy, defaultCreditPolicy } from "./consumer_credit_policy"
import { Message } from "./publisher"
import { Offset } from "./requests/subscribe_request"

export type ConsumerFunc = (message: Message) => void
export const computeExtendedConsumerId = (consumerId: number, connectionId: string) => {
  return `${consumerId}@${connectionId}`
}

export interface Consumer {
  close(manuallyClose: boolean): Promise<void>
  storeOffset(offsetValue: bigint): Promise<void>
  queryOffset(): Promise<bigint>
  getConnectionInfo(): ConnectionInfo
  consumerId: number
  consumerRef?: string
  readonly extendedId: string
}

export class StreamConsumer implements Consumer {
  private connection: Connection
  private stream: string
  public consumerId: number
  public consumerRef?: string
  public offset: Offset
  private clientLocalOffset: Offset
  private creditsHandler: ConsumerCreditPolicy
  private consumerHandle: ConsumerFunc
  private closed: boolean

  constructor(
    handle: ConsumerFunc,
    params: {
      connection: Connection
      stream: string
      consumerId: number
      consumerRef?: string
      offset: Offset
      creditPolicy?: ConsumerCreditPolicy
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
    this.closed = false
  }

  async close(manuallyClose: boolean): Promise<void> {
    this.closed = true
    this.connection.decrRefCount()
    if (ConnectionPool.removeIfUnused(this.connection)) {
      await this.connection.close({ closingCode: 0, closingReason: "", manuallyClose })
    }
  }

  public storeOffset(offsetValue: bigint): Promise<void> {
    if (!this.consumerRef) throw new Error("ConsumerReference must be defined in order to use this!")
    return this.connection.storeOffset({ stream: this.stream, reference: this.consumerRef, offsetValue })
  }

  public queryOffset(): Promise<bigint> {
    if (!this.consumerRef) throw new Error("ConsumerReference must be defined in order to use this!")
    return this.connection.queryOffset({ stream: this.stream, reference: this.consumerRef })
  }

  public getConnectionInfo(): ConnectionInfo {
    const { host, port, id, readable, localPort, ready } = this.connection.getConnectionInfo()
    return { host, port, id, readable, localPort, ready }
  }

  public get localOffset() {
    return this.clientLocalOffset.clone()
  }

  public handle(message: Message) {
    if (this.closed || this.isMessageOffsetLessThanConsumers(message)) return
    this.consumerHandle(message)
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

  private maybeUpdateLocalOffset(message: Message) {
    if (message.offset !== undefined) this.clientLocalOffset = Offset.offset(message.offset)
  }

  // TODO -- Find better name?
  private isMessageOffsetLessThanConsumers(message: Message) {
    return this.offset.type === "numeric" && message.offset !== undefined && message.offset < this.offset.value!
  }
}
