import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class PeerPropertiesResponse extends AbstractResponse {
  static key = 0x8011
  static readonly Version = 1

  readonly properties: Record<string, string> = {}

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(PeerPropertiesResponse)

    const howMany = this.response.payload.readInt32()
    for (let index = 0; index < howMany; index++) {
      const resKey = this.response.payload.readString()
      const resValue = this.response.payload.readString()
      this.properties[resKey] = resValue
    }
  }
}
