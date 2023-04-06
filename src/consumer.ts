import { Message } from "./producer"

export type ConsumerFunc = (message: Message) => void

export class Consumer {
  constructor(readonly handle: ConsumerFunc) {}

  async close(): Promise<void> {
    throw new Error("Method not implemented.")
  }
}
