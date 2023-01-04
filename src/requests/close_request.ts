import { CloseResponse } from "../responses/close_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class CloseRequest extends AbstractRequest {
  readonly responseKey = CloseResponse.key
  readonly key = 0x0016

  constructor(private params: { closingCode: number; closingReason: string }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeUInt16(this.params.closingCode)
    writer.writeString(this.params.closingReason)
  }
}
