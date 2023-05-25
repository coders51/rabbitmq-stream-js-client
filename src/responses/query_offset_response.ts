import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class QueryOffsetResponse extends AbstractResponse {
  static key = 0x800b
  readonly offsetValue: bigint

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(QueryOffsetResponse)
    this.offsetValue = response.payload.readUInt64()
  }
}
