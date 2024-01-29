import { AbstractRequest } from "./abstract_request"
import { DeleteStreamResponse } from "../responses/delete_stream_response"
import { DataWriter } from "./data_writer"

export class DeleteStreamRequest extends AbstractRequest {
  static readonly Key = 0x000e
  readonly key = DeleteStreamRequest.Key
  static readonly Version = 1
  readonly responseKey = DeleteStreamResponse.key
  private readonly stream: string

  constructor(stream: string) {
    super()
    this.stream = stream
  }

  protected writeContent(writer: DataWriter): void {
    writer.writeString(this.stream)
  }
}
