import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class SaslHandshakeResponse extends AbstractResponse {
  static key = 0x8012
  static readonly Version = 1

  readonly mechanisms: string[] = []

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(SaslHandshakeResponse)

    const numOfMechanisms = this.response.payload.readInt32()
    for (let index = 0; index < numOfMechanisms; index++) {
      const mechanism = this.response.payload.readString()
      this.mechanisms.push(mechanism)
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
