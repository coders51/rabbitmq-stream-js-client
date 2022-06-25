import { RawResponse } from "./raw_response"
import { readString, Response } from "./response"

export class SaslHandshakeResponse implements Response {
  static key = 0x8012
  readonly mechanisms: string[] = []

  constructor(private response: RawResponse) {
    if (response.key !== SaslHandshakeResponse.key)
      throw new Error(`Unable to create SaslHandshakeResponse from data of type ${response.key}`)

    let offset = 0
    const numOfMechanisms = this.response.payload.readUint32BE(offset)
    offset += 4
    for (let index = 0; index < numOfMechanisms; index++) {
      const res = readString(this.response.payload, offset)
      offset = res.offset
      this.mechanisms.push(res.value)
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
}
