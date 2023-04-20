import { Connection } from "./connection"
import { Message } from "./producer"

export type ConsumerFunc = (message: Message) => void

export class Consumer {
  private connection: Connection
  private stream: string
  private consumerRef: string
  constructor(
    params: {
      connection: Connection
      stream: string
      consumerRef?: string
    },
    readonly handle: ConsumerFunc
  ) {
    this.connection = params.connection
    this.stream = params.stream
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
