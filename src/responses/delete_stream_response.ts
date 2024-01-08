import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class DeleteStreamResponse extends AbstractResponse {
  static key = 0x800e
  static readonly Version = 1

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(DeleteStreamResponse)
  }
}
