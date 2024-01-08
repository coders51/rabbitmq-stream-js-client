import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export interface Statistics {
  committedChunkId: bigint
  firstChunkId: bigint
  lastChunkId: bigint
}

export class StreamStatsResponse extends AbstractResponse {
  static key = 0x801c
  static readonly Version = 1

  private rawStats: Record<string, bigint> = {}
  readonly statistics: Statistics = {
    committedChunkId: BigInt(0),
    firstChunkId: BigInt(0),
    lastChunkId: BigInt(0),
  }

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(StreamStatsResponse)

    const stats = this.response.payload.readInt32()
    for (let i = 0; i < stats; i++) {
      const statKey = this.response.payload.readString()
      const statVal = this.response.payload.readInt64()
      this.rawStats[statKey] = statVal
    }

    this.statistics.committedChunkId = this.rawStats["committed_chunk_id"]
    this.statistics.firstChunkId = this.rawStats["first_chunk_id"]
    this.statistics.lastChunkId = this.rawStats["last_chunk_id"]
  }
}
