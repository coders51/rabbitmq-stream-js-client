import { Compression } from "../compression"
import { Message } from "../producer"
import { AbstractRequest } from "./abstract_request"
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
    writer.writeUInt8(this.params.publisherId)
    // number of root messages. In this case will be always 1
    writer.writeUInt32(1)
    writer.writeUInt64(this.params.publishingId)
    writer.writeByte(0x80 | (1 << this.params.compression.type))
    writer.writeUInt16(this.params.compression.messageCount())
    writer.writeUInt32(this.params.compression.UnCompressedSize())
    writer.writeUInt32(this.params.compression.CompressedSize())
    this.params.compression.writeContent(writer)
  }
}
