import { Message } from "./producer"

export class Consumer {
  constructor(readonly handle: (message: Message) => void) {}

  async close(): Promise<void> {
    throw new Error("Method not implemented.")
  }
}
