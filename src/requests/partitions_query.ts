import { PartitionsResponse } from "../responses/partitions_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class PartitionsQuery extends AbstractRequest {
  static readonly Key = 0x0019
  static readonly Version = 1

  constructor(private params: { superStream: string }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.params.superStream)
  }

  get key(): number {
    return PartitionsQuery.Key
  }
  get responseKey(): number {
    return PartitionsResponse.key
  }
  get version(): number {
    return PartitionsQuery.Version
  }
}
