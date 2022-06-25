import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"
import { readString } from "./response"

export class SaslHandshakeResponse extends AbstractResponse {
  static key = 0x8012
  readonly mechanisms: string[] = []

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(SaslHandshakeResponse)

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
