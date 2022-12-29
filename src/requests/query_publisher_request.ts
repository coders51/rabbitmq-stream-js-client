import { AbstractRequest } from "./abstract_request"
import { QueryPublisherResponse } from "../responses/query_publisher_response"
import { DataWriter } from "./data_writer"

export class QueryPublisherRequest extends AbstractRequest {
  readonly key = 0x0005
  readonly responseKey = QueryPublisherResponse.key

  constructor(private params: { stream: string; publisherRef: string }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.params.publisherRef)
    writer.writeString(this.params.stream)
  }
}
