import { RawResponse } from "./raw_response"
import { Response } from "./response"

export class SaslAuthenticateResponse implements Response {
  static key = 0x8013

  constructor(private response: RawResponse) {
    if (response.key !== SaslAuthenticateResponse.key)
      throw new Error(`Unable to create SaslAuthenticateResponse from data of type ${response.key}`)
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
