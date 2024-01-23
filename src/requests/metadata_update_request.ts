import { MetadataInfo } from "../responses/raw_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class MetadataUpdateRequest extends AbstractRequest {
  static readonly Key = 0x0010
  static readonly Version = 1

  constructor(private params: { metadataInfo: MetadataInfo }) {
    super()
  }

  writeContent(b: DataWriter) {
    b.writeUInt16(this.params.metadataInfo.code)
    b.writeString(this.params.metadataInfo.stream)
  }

  get key(): number {
    return MetadataUpdateRequest.Key
  }
  get responseKey(): number {
    return -1
  }
  get version(): number {
    return MetadataUpdateRequest.Version
  }
}
