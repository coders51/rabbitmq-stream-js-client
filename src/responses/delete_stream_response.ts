import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class DeleteStreamResponse extends AbstractResponse {
  static key = 0x000e

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(DeleteStreamResponse)
  }
}
