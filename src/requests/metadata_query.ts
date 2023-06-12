import { AbstractRequest } from "./abstract_request"
import { MetadataResponse } from "../responses/metadata_response"
import { DataWriter } from "./data_writer"

export class MetadataQuery extends AbstractRequest {
  readonly key = 0x000f
  readonly responseKey = MetadataResponse.key

  constructor(private streamName: string) {
    console.log(" *********************************** creating metadata query ")
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.streamName)
  }
}
