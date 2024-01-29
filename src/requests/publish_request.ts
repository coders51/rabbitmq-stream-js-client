import { amqpEncode } from "../amqp10/encoder"
import { Message } from "../publisher"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export type PublishRequestMessage = {
  publishingId: bigint
  filterValue?: string
  message: Message
}

interface PublishRequestParams {
  publisherId: number
  messages: Array<PublishRequestMessage>
}

export class PublishRequest extends AbstractRequest {
  static readonly Key = 0x02
  static readonly Version = 1
  readonly key = PublishRequest.Key
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
