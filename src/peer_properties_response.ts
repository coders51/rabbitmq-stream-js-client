import { RawResponse } from "./raw_response"
import { Response } from "./response"

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

function readString(payload: Buffer, offset: number): { offset: number; value: string } {
  const size = payload.readUInt16BE(offset)
  const value = payload.toString("utf8", offset + 2, offset + 2 + size)
  return { offset: offset + 2 + size, value }
}
