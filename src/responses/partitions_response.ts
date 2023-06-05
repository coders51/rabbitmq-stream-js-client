import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class PartitionsResponse extends AbstractResponse {
  static key = 0x8019
  public streams: string[] = []

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(PartitionsResponse)

    const numStreams = this.response.payload.readInt32()
    for (let i = 0; i < numStreams; i++) {
      this.streams.push(this.response.payload.readString())
    }
  }
}
