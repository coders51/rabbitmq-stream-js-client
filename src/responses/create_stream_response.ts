import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class CreateStreamResponse extends AbstractResponse {
  static key = 0x000d

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(CreateStreamResponse)
  }
}
