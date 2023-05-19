import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class StreamStatsResponse extends AbstractResponse {
  static key = 0x801c
  readonly statistics: Record<string, bigint> = {}

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(StreamStatsResponse)

    const stats = this.response.payload.readInt32()
    for (let i = 0; i < stats; i++) {
      const statKey = this.response.payload.readString()
      const statVal = this.response.payload.readInt64()
      this.statistics[statKey] = statVal
    }
  }
}
