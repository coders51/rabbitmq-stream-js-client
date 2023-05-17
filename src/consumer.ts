import { Connection } from "./connection"
import { Message } from "./producer"

export type ConsumerFunc = (message: Message) => void

export class Consumer {
  private connection: Connection
  private stream: string
  public consumerId: number
  private consumerRef: string

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
    this.consumerRef = params.consumerRef || ""
  }

  async close(): Promise<void> {
    throw new Error("Method not implemented.")
  }

  public storeOffset(offsetValue: bigint): Promise<void> {
    return this.connection.storeOffset({ stream: this.stream, reference: this.consumerRef, offsetValue })
  }
}
