import { DeleteSuperStreamResponse } from "../responses/delete_super_stream_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class DeleteSuperStreamRequest extends AbstractRequest {
  static readonly Key = 0x001e
  readonly key = DeleteSuperStreamRequest.Key
  static readonly Version = 1
  readonly responseKey = DeleteSuperStreamResponse.key
  private readonly streamName: string

  constructor(streamName: string) {
    super()
    this.streamName = streamName
  }

  protected writeContent(writer: DataWriter): void {
    writer.writeString(this.streamName)
  }
}
