import { MetadataResponse } from "../responses/metadata_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class MetadataRequest extends AbstractRequest {
  readonly responseKey = MetadataResponse.key
  readonly key = 0x000f

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
