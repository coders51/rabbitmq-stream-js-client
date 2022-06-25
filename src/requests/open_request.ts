import { OpenResponse } from "../responses/open_response"
import { AbstractRequest, writeString } from "./abstract_request"

export class OpenRequest extends AbstractRequest {
  readonly responseKey = OpenResponse.key
  readonly key = 0x0015

  constructor(private params: { virtualHost: string }) {
    super()
  }

  writeContent(b: Buffer, offset: number): number {
    return writeString(b, offset, this.params.virtualHost)
  }
}
