import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class StoreOffsetResponse extends AbstractResponse {
  static key = 0x000a

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(StoreOffsetResponse)
  }
}
