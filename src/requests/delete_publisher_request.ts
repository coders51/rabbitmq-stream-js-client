import { DeletePublisherResponse } from "../responses/delete_publisher_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class DeletePublisherRequest extends AbstractRequest {
  static readonly Key = 0x0006
  static readonly Version = 1

  constructor(private publisherId: number) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeUInt8(this.publisherId)
  }

  get key(): number {
    return DeletePublisherRequest.Key
  }
  get responseKey(): number {
    return DeletePublisherResponse.key
  }
  get version(): number {
    return DeletePublisherRequest.Version
  }
}
