import { PartitionsResponse } from "../responses/partitions_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class PartitionsQuery extends AbstractRequest {
  readonly responseKey = PartitionsResponse.key
  readonly key = 0x0019

  constructor(private params: { superStream: string }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.params.superStream)
  }
}
