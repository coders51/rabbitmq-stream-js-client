// import { Connection } from "./connection"
import { Message } from "./producer"
// import { Offset } from "./requests/subscribe_request"

export class Consumer {
  // private connection: Connection
  // private stream: string
  // private offset: Offset
  // private consumerId: number
  handle: (message: Message) => void

  constructor(
    // params: {
    //   connection: Connection
    //   stream: string
    //   offset: Offset
    //   consumerId: number
    // },
    handle: (message: Message) => void
  ) {
    // this.connection = params.connection
    // this.stream = params.stream
    // this.offset = params.offset
    // this.consumerId = params.consumerId
    this.handle = handle
  }

  async close(): Promise<void> {
    throw new Error("Method not implemented.")
  }
}
