import { amqpEncode } from "../amqp10/encoder"
import { Message } from "../producer"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export type PublishRequestMessage = {
  publishingId: bigint
  message: Message
}

interface PublishRequestParams {
  publisherId: number
  messages: Array<PublishRequestMessage>
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
      amqpEncode(writer, message)
    })
  }
}
