import { Version } from "../versions"
import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class ExchangeCommandVersionsResponse extends AbstractResponse {
  static key = 0x801b
  static readonly Version = 1

  readonly serverDeclaredVersions: Version[]

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(ExchangeCommandVersionsResponse)
    this.serverDeclaredVersions = [] as Version[]
    const serverDeclaredVersionsCount = response.payload.readInt32()

    for (let i: number = 0; i < serverDeclaredVersionsCount; i++) {
      const declaredVersion: Version = {
        key: response.payload.readUInt16(),
        minVersion: response.payload.readUInt16(),
        maxVersion: response.payload.readUInt16(),
      }
      this.serverDeclaredVersions.push(declaredVersion)
    }
  }
}
