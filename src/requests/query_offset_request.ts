import { QueryOffsetResponse } from "../responses/query_offset_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class QueryOffsetRequest extends AbstractRequest {
  static readonly Key = 0x000b
  static readonly Version = 1
  private readonly reference: string
  private readonly stream: string

  constructor(params: { reference: string; stream: string }) {
    super()
    this.stream = params.stream
    this.reference = params.reference
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.reference)
    writer.writeString(this.stream)
  }

  get key(): number {
    return QueryOffsetRequest.Key
  }
  get responseKey(): number {
    return QueryOffsetResponse.key
  }
  get version(): number {
    return QueryOffsetRequest.Version
  }
}
