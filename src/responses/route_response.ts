import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class RouteResponse extends AbstractResponse {
  static key = 0x8018
  public streams: string[] = []

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(RouteResponse)

    const numStreams = this.response.payload.readUInt32()
    for (let i = 0; i < numStreams; i++) {
      this.streams.push(this.response.payload.readString())
    }
  }
}
