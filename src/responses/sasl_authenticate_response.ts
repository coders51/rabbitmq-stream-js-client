import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class SaslAuthenticateResponse extends AbstractResponse {
  static key = 0x8013
  static MinVersion = 1
  static MaxVersion = 1

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(SaslAuthenticateResponse)
  }

  get data(): string {
    // TODO how to manage this data??
    return this.response.payload.toString()
  }
}
