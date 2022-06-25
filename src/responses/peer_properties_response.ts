import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"
import { readString } from "./response"

export class PeerPropertiesResponse extends AbstractResponse {
  static key = 0x8011
  readonly properties: Record<string, string> = {}

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(PeerPropertiesResponse)

    let offset = 0
    const howMany = this.response.payload.readInt32BE(offset)
    offset += 4
    for (let index = 0; index < howMany; index++) {
      const resKey = readString(this.response.payload, offset)
      offset = resKey.offset
      const resValue = readString(this.response.payload, offset)
      offset = resValue.offset
      this.properties[resKey.value] = resValue.value
    }
  }
}
