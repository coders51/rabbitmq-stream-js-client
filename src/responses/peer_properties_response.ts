import { RawResponse } from "./raw_response"
import { readString, Response } from "./response"

export class PeerPropertiesResponse implements Response {
  static key = 0x8011
  readonly properties: Record<string, string> = {}

  constructor(private response: RawResponse) {
    if (response.key !== PeerPropertiesResponse.key)
      throw new Error(`Unable to create PeerPropertiesResponse from data of type ${response.key}`)

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

  get key() {
    return this.response.key
  }

  get correlationId(): number {
    return this.response.correlationId
  }

  get code(): number {
    return this.response.code
  }
  get ok(): boolean {
    return this.code === 0x01
  }
}
