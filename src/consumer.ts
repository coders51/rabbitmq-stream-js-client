import { Client } from "./client"
import { Message } from "./producer"

export type ConsumerFunc = (message: Message) => void

export interface Consumer {
  close(): Promise<void>
  storeOffset(offsetValue: bigint): Promise<void>
  queryOffset(): Promise<bigint>
  getConnectionInfo(): { host: string; port: number; id: string }
  consumerId: number
  consumerRef?: string
}

export class StreamConsumer implements Consumer {
  private client: Client
  private stream: string
  public consumerId: number
  public consumerRef?: string

  constructor(
    readonly handle: ConsumerFunc,
    params: {
      client: Client
      stream: string
      consumerId: number
      consumerRef?: string
    }
  ) {
    this.client = params.client
    this.stream = params.stream
    this.consumerId = params.consumerId
    this.consumerRef = params.consumerRef
  }

  async close(): Promise<void> {
    await this.client.close()
  }

  public storeOffset(offsetValue: bigint): Promise<void> {
    if (!this.consumerRef) throw new Error("ConsumerReference must be defined in order to use this!")
    return this.client.storeOffset({ stream: this.stream, reference: this.consumerRef, offsetValue })
  }

  public queryOffset(): Promise<bigint> {
    if (!this.consumerRef) throw new Error("ConsumerReference must be defined in order to use this!")
    return this.client.queryOffset({ stream: this.stream, reference: this.consumerRef })
  }

  public getConnectionInfo(): { host: string; port: number; id: string } {
    return this.client.getConnectionInfo()
  }
}
