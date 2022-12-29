import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class QueryPublisherResponse extends AbstractResponse {
  static key = 0x8005
  readonly sequence: bigint = 0n

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(QueryPublisherResponse)
    this.sequence = this.response.payload.readUInt64()
  }
}
