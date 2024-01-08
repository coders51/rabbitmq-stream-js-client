import { Version } from "../versions"
import { ExchangeCommandVersionsResponse } from "../responses/exchange_command_versions_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class ExchangeCommandVersionsRequest extends AbstractRequest {
  static readonly Key = 0x001b
  readonly key = ExchangeCommandVersionsRequest.Key
  static readonly Version = 1
  readonly responseKey = ExchangeCommandVersionsResponse.key
  constructor(readonly versions: Version[]) {
    super()
  }

  writeContent(writer: DataWriter): void {
    writer.writeInt32(this.versions.length)
    this.versions.forEach((entry: Version) => {
      writer.writeUInt16(entry.key)
      writer.writeUInt16(entry.minVersion)
      writer.writeUInt16(entry.maxVersion)
    })
  }
}
