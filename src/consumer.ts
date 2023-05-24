import { Connection } from "./connection"
import { Message } from "./producer"

export type ConsumerFunc = (message: Message) => void

export class Consumer {
  private connection: Connection
  private stream: string
  public consumerId: number
  public consumerRef?: string

  constructor(
    readonly handle: ConsumerFunc,
    params: {
      connection: Connection
      stream: string
      consumerId: number
      consumerRef?: string
    }
  ) {
    this.connection = params.connection
    this.stream = params.stream
    this.consumerId = params.consumerId
    this.consumerRef = params.consumerRef
  }

  async close(): Promise<void> {
    throw new Error("Method not implemented.")
  }

  public storeOffset(offsetValue: bigint): Promise<void> {
    if (!this.consumerRef) throw new Error("ConsumerReference must be defined in order to use this!")
    return this.connection.storeOffset({ stream: this.stream, reference: this.consumerRef, offsetValue })
  }

  public async queryOffset(): Promise<bigint> {
    if (!this.consumerRef) throw new Error("ConsumerReference must be defined in order to use this!")
    const offset = await this.connection.queryOffset({ stream: this.stream, reference: this.consumerRef })
    return offset
  }
}
