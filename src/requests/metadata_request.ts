import { MetadataResponse } from "../responses/metadata_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class MetadataRequest extends AbstractRequest {
  readonly responseKey = MetadataResponse.key
  static readonly Key = 0x000f
  static readonly Version = 1
  readonly key = MetadataRequest.Key

  constructor(private params: { streams: string[] }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeInt32(this.params.streams.length)
    this.params.streams.forEach((s) => {
      writer.writeString(s)
    })
  }
}
