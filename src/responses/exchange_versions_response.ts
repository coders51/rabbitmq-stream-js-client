import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class ExchangeCommandVersionsResponse extends AbstractResponse {
  static key = 0x801b
  readonly commandKey: number
  readonly minVersion: number
  readonly maxVersion: number

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(ExchangeCommandVersionsResponse)

    this.commandKey = this.response.payload.readUInt16()
    this.minVersion = this.response.payload.readUInt16()
    this.maxVersion = this.response.payload.readUInt16()
  }
}
