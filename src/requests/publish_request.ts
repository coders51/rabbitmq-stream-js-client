import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

interface PublishRequestParams {
  publisherId: number
  messages: Array<{ publishingId: bigint; message: Buffer }>
}

export class PublishRequest extends AbstractRequest {
  readonly key = 0x02
  readonly responseKey = -1

  constructor(private params: PublishRequestParams) {
    super()
  }

  protected writeContent(writer: DataWriter): void {
    writer.writeUInt8(this.params.publisherId)
    writer.writeUInt32(this.params.messages.length)
    this.params.messages.forEach(({ publishingId, message }) => {
      writer.writeUInt64(publishingId)
      writer.writeUInt32(message.length)
      writer.writeData(message)
    })
  }
}
