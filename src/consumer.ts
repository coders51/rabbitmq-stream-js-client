import { Connection } from "./connection"
import { Message } from "./producer"

export type ConsumerFunc = (message: Message) => void

export class Consumer {
  private connection: Connection
  private stream: string
  private consumerRef: string
  public consumerId: number

  constructor(
    params: {
      connection: Connection
      stream: string
      consumerId: number
      consumerRef?: string
    },
    readonly handle: ConsumerFunc
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

  public queryOffset() {
    return this.connection.queryOffset({ reference: this.consumerRef, stream: this.stream })
  }

  public getConsumerRef() {
    return this.consumerRef
  }
}
