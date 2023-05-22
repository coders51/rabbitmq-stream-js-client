import { DeletePublisherResponse } from "../responses/delete_publisher_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class DeletePublisherRequest extends AbstractRequest {
  readonly responseKey = DeletePublisherResponse.key
  readonly key = 0x0006

  constructor(private publisherId: number) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeUInt8(this.publisherId)
  }
}
