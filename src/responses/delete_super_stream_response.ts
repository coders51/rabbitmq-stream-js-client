import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class DeleteSuperStreamResponse extends AbstractResponse {
  static key = 0x801e
  static readonly Version = 1

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(DeleteSuperStreamResponse)
  }
}
