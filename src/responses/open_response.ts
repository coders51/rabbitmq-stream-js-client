import { RawResponse } from "./raw_response"
import { readString, Response } from "./response"

export class OpenResponse implements Response {
  static key = 0x8015
  readonly properties: Record<string, string> = {}

  constructor(private response: RawResponse) {
    if (response.key !== OpenResponse.key)
      throw new Error(`Unable to create OpenResponse from data of type ${response.key}`)

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

  public get correlationId(): number {
    return this.response.correlationId
  }

  get code(): number {
    return this.response.code
  }

  get ok(): boolean {
    return this.code === 0x01
  }

  get data(): string {
    // TODO how to manage this data??
    return this.response.payload.toString()
  }
}
