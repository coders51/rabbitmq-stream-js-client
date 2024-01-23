import { AbstractRequest } from "./abstract_request"
import { QueryPublisherResponse } from "../responses/query_publisher_response"
import { DataWriter } from "./data_writer"

export class QueryPublisherRequest extends AbstractRequest {
  static readonly Key = 0x0005
  static readonly Version = 1

  constructor(private params: { stream: string; publisherRef: string }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.params.publisherRef)
    writer.writeString(this.params.stream)
  }

  get key(): number {
    return QueryPublisherRequest.Key
  }
  get responseKey(): number {
    return QueryPublisherResponse.key
  }
  get version(): number {
    return QueryPublisherRequest.Version
  }
}
