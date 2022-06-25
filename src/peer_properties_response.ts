import { RawResponse } from "./raw_response"
import { Response } from "./response"

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
      const xyz = readString(this.response.payload, offset)
      offset = xyz.offset
      this.mechanisms.push(xyz.value)
    }
  }

  get key() {
    return this.response.key
  }

  public get correlationId(): number {
    return this.response.correlationId
  }

  get code(): number {
    return this.code
  }
  get ok(): boolean {
    return this.code === 0x01
  }
}

export class PeerPropertiesResponse implements Response {
  static key = 0x8011

  constructor(private response: RawResponse) {
    if (response.key !== PeerPropertiesResponse.key)
      throw new Error(`Unable to create PeerPropertiesResponse from data of type ${response.key}`)
  }

  get key() {
    return this.response.key
  }

  get correlationId(): number {
    return this.response.correlationId
  }

  get code(): number {
    return this.code
  }
  get ok(): boolean {
    return this.code === 0x01
  }
}

function readString(payload: Buffer, offset: number): { offset: number; value: string } {
  const size = payload.readUInt16BE(offset)
  const value = payload.toString("utf8", offset + 2, offset + 2 + size)
  return { offset: offset + 2 + size, value }
}
