import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class OpenResponse extends AbstractResponse {
  static key = 0x8015
  static readonly Version = 1

  readonly properties: Record<string, string> = {}

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(OpenResponse)

    const howMany = this.response.payload.readInt32()
    for (let index = 0; index < howMany; index++) {
      const resKey = this.response.payload.readString()
      const resValue = this.response.payload.readString()
      this.properties[resKey] = resValue
    }
  }

  get data(): string {
    // TODO how to manage this data??
    return this.response.payload.toString()
  }
}
