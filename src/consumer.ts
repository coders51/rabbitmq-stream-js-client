import { Connection } from "./connection"
import { Offset } from "./requests/subscribe_request"

export class Consumer {
  private connection: Connection
  private stream: string
  private offset: Offset
  private consumerId: number

  constructor(params: {
    connection: Connection
    stream: string
    offset: Offset
    consumerId: number
  }){
    this.connection = params.connection
    this.stream = params.stream
    this.offset = params.offset
    this.consumerId = params.consumerId
  }

  async close(): Promise<void> {
    throw new Error("Method not implemented.")
  }
}
