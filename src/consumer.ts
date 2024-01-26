import { ConnectionInfo, Connection } from "./connection"
import { ConnectionPool } from "./connection_pool"
import { Message } from "./publisher"
import { Offset } from "./requests/subscribe_request"

export type ConsumerFunc = (message: Message) => void

export interface Consumer {
  close(): Promise<void>
  storeOffset(offsetValue: bigint): Promise<void>
  queryOffset(): Promise<bigint>
  getConnectionInfo(): ConnectionInfo
  consumerId: number
  consumerRef?: string
}

export class StreamConsumer implements Consumer {
  private connection: Connection
  private stream: string
  public consumerId: number
  public consumerRef?: string
  public offset: Offset

  constructor(
    readonly handle: ConsumerFunc,
    params: {
      connection: Connection
      stream: string
      consumerId: number
      consumerRef?: string
      offset: Offset
    }
  ) {
    this.connection = params.connection
    this.stream = params.stream
    this.consumerId = params.consumerId
    this.consumerRef = params.consumerRef
    this.offset = params.offset
    this.connection.incrRefCount()
  }

  async close(): Promise<void> {
    this.connection.decrRefCount()
    if (ConnectionPool.removeIfUnused(this.connection)) {
      await this.connection.close()
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
    const { host, port, id, readable, localPort } = this.connection.getConnectionInfo()
    return { host, port, id, readable, localPort }
  }
}
