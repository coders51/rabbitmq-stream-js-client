import { OpenResponse } from "../responses/open_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class OpenRequest extends AbstractRequest {
  readonly responseKey = OpenResponse.key
  static readonly Key = 0x0015
  static readonly Version = 1
  readonly key = OpenRequest.Key

  constructor(private params: { virtualHost: string }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.params.virtualHost)
  }
}
