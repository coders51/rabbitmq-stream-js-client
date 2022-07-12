import { AbstractRequest } from "./abstract_request"
import { DeleteStreamResponse } from "../responses/delete_stream_response"
import { DataWriter } from "./sasl_authenticate_request"

export class DeleteStreamRequest extends AbstractRequest {
  readonly key = 0x000e
  readonly responseKey = DeleteStreamResponse.key

  constructor(private stream: string) {
    super()
  }

  protected writeContent(writer: DataWriter): void {
    writer.writeString(this.stream)
  }
}
