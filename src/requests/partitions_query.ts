import { PartitionsResponse } from "../responses/partitions_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class PartitionsQuery extends AbstractRequest {
  readonly responseKey = PartitionsResponse.key
  static readonly Key = 0x0019
  static readonly Version = 1
  readonly key = PartitionsQuery.Key

  constructor(private params: { superStream: string }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.params.superStream)
  }
}
