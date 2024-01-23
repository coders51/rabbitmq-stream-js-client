import { Version } from "../versions"
import { ExchangeCommandVersionsResponse } from "../responses/exchange_command_versions_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class ExchangeCommandVersionsRequest extends AbstractRequest {
  static readonly Key = 0x001b
  static readonly Version = 1
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

  get key(): number {
    return ExchangeCommandVersionsRequest.Key
  }
  get responseKey(): number {
    return ExchangeCommandVersionsResponse.key
  }
  get version(): number {
    return ExchangeCommandVersionsRequest.Version
  }
}
