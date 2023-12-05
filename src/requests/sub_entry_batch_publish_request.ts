import { amqpEncode, messageSize } from "../amqp10/encoder"
import { Compression } from "../compression"
import { Message } from "../producer"
import { AbstractRequest, BufferDataWriter } from "./abstract_request"
import { DataWriter } from "./data_writer"

interface SubEntryBatchPublishRequestParams {
  publisherId: number
  publishingId: bigint
  compression: Compression
  messages: Message[]
}

export class SubEntryBatchPublishRequest extends AbstractRequest {
  readonly key = 0x02
  readonly responseKey = -1

  constructor(private params: SubEntryBatchPublishRequestParams) {
    super()
  }

  protected writeContent(writer: DataWriter): void {
    const { compression, messages, publishingId, publisherId } = this.params
    writer.writeUInt8(publisherId)
    // number of root messages. In this case will be always 1
    writer.writeUInt32(1)
    writer.writeUInt64(publishingId)
    writer.writeByte(0x80 | (compression.getType() << 4))
    writer.writeUInt16(messages.length)
    writer.writeUInt32(messages.reduce((sum, message) => sum + 4 + messageSize(message), 0))

    const data = new BufferDataWriter(Buffer.alloc(1024), 0)
    messages.forEach((m) => amqpEncode(data, m))

    const compressedData = compression.compress(data.toBuffer())

    writer.writeUInt32(compressedData.length)
    writer.writeData(compressedData)
  }
}
