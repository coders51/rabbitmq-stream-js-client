import { MetadataInfo } from "../responses/raw_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class MetadataUpdateRequest extends AbstractRequest {
  readonly responseKey = -1
  readonly key = 0x0010

  constructor(private params: { metadataInfo: MetadataInfo }) {
    super()
  }

  writeContent(b: DataWriter) {
    b.writeUInt16(this.params.metadataInfo.code)
    b.writeString(this.params.metadataInfo.stream)
  }
}
