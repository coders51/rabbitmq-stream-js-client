import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class StoreOffsetResponse extends AbstractResponse {
  static key = 0x000a
  static MinVersion = 1
  static MaxVersion = 1

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(StoreOffsetResponse)
  }
}
