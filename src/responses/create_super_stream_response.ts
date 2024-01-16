import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class CreateSuperStreamResponse extends AbstractResponse {
  static key = 0x801d
  static readonly Version = 1

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(CreateSuperStreamResponse)
  }
}
