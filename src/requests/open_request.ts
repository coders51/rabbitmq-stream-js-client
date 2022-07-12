import { OpenResponse } from "../responses/open_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class OpenRequest extends AbstractRequest {
  readonly responseKey = OpenResponse.key
  readonly key = 0x0015

  constructor(private params: { virtualHost: string }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.params.virtualHost)
  }
}
