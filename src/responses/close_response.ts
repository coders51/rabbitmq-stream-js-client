import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class CloseResponse extends AbstractResponse {
  static key = 0x8016
  static readonly Version = 1

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(CloseResponse)
  }
}
