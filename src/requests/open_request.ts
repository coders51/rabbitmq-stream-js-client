import { OpenResponse } from "../responses/open_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./sasl_authenticate_request"

export class OpenRequest extends AbstractRequest {
  readonly responseKey = OpenResponse.key
  readonly key = 0x0015

  constructor(private params: { virtualHost: string }) {
    super()
  }

  writeContent(b: DataWriter) {
    b.writeString(this.params.virtualHost)
  }
}
