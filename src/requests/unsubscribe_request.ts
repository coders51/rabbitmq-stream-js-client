import { UnsubscribeResponse } from "../responses/unsubscribe_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class UnsubscribeRequest extends AbstractRequest {
  readonly key = 0x000c
  readonly responseKey = UnsubscribeResponse.key

  constructor(private subscriptionId: number) {
    super()
  }

  protected writeContent(writer: DataWriter): void {
    writer.writeUInt8(this.subscriptionId)
  }
}
