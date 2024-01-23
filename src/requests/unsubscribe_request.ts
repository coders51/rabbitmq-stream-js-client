import { UnsubscribeResponse } from "../responses/unsubscribe_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class UnsubscribeRequest extends AbstractRequest {
  static readonly Key = 0x000c
  static readonly Version = 1

  constructor(private subscriptionId: number) {
    super()
  }

  protected writeContent(writer: DataWriter): void {
    writer.writeUInt8(this.subscriptionId)
  }

  get key(): number {
    return UnsubscribeRequest.Key
  }
  get responseKey(): number {
    return UnsubscribeResponse.key
  }
  get version(): number {
    return UnsubscribeRequest.Version
  }
}
