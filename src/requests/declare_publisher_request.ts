import { DeclarePublisherResponse } from "../responses/declare_publisher_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class DeclarePublisherRequest extends AbstractRequest {
  readonly responseKey = DeclarePublisherResponse.key
  readonly key = 0x0001

  constructor(private params: { stream: string; publisherId: number; publisherRef?: string }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeUInt8(this.params.publisherId)
    writer.writeString(this.params.publisherRef || "")
    writer.writeString(this.params.stream)
  }
}
