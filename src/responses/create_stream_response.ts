import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class CreateStreamResponse extends AbstractResponse {
  static key = 0x800d
  static readonly Version = 1

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(CreateStreamResponse)
  }
}
