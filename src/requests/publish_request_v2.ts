import { amqpEncode } from "../amqp10/encoder"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"
import { PublishRequestMessage } from "./publish_request"

interface PublishRequestParams {
  publisherId: number
  messages: Array<PublishRequestMessage>
}

export class PublishRequestV2 extends AbstractRequest {
  static readonly Key = 0x02
  static readonly Version = 2
  readonly key = PublishRequestV2.Key
  readonly responseKey = -1

  constructor(private params: PublishRequestParams) {
    super()
  }

  protected writeContent(writer: DataWriter): void {
    writer.writeUInt8(this.params.publisherId)
    writer.writeUInt32(this.params.messages.length)
    this.params.messages.forEach(({ publishingId, filterValue, message }) => {
      writer.writeUInt64(publishingId)
      filterValue ? writer.writeString(filterValue) : writer.writeInt16(-1)
      amqpEncode(writer, message)
    })
  }

  get version(): number {
    return PublishRequestV2.Version
  }
}
