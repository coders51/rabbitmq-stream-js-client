import { Connection } from "./connection"
import { Offset } from "./requests/subscribe_request"

export class Consumer {
  private constructor(
    private readonly connection: Connection,
    private readonly streamName: string,
    private readonly offset: Offset,
    private readonly messageHandler: (message: any) => Promise<void>
  ) {}

  static create(params: {
    connection: Connection
    streamName: string
    offset: Offset
    messageHandler: (message: any) => Promise<void>
  }): Consumer {
    return new Consumer(params.connection, params.streamName, params.offset, params.messageHandler)

    // subscribe su connection per inizializzare la connessione

    // ricevere i messaggi sempre via connection e chimare l'handler sull messaggio
  }

  async close(): Promise<void> {
    throw new Error("Method not implemented.")
  }
}
