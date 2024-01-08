import { CloseResponse } from "../responses/close_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class CloseRequest extends AbstractRequest {
  readonly responseKey = CloseResponse.key
  static readonly Key = 0x0016
  static readonly Version = 1
  readonly key = CloseRequest.Key

  constructor(private params: { closingCode: number; closingReason: string }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeUInt16(this.params.closingCode)
    writer.writeString(this.params.closingReason)
  }
}
