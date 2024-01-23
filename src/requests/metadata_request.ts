import { MetadataResponse } from "../responses/metadata_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class MetadataRequest extends AbstractRequest {
  static readonly Key = 0x000f
  static readonly Version = 1

  constructor(private params: { streams: string[] }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeInt32(this.params.streams.length)
    this.params.streams.forEach((s) => {
      writer.writeString(s)
    })
  }

  get key(): number {
    return MetadataRequest.Key
  }
  get responseKey(): number {
    return MetadataResponse.key
  }
  get version(): number {
    return MetadataRequest.Version
  }
}
