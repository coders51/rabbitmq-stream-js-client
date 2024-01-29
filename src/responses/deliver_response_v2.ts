import { Message } from "../publisher"
import { BufferDataWriter } from "../requests/buffer_data_writer"
import { RawDeliverResponseV2 } from "./raw_response"
import { Response } from "./response"

export class DeliverResponseV2 implements Response {
  static key = 0x0008
  static readonly Version = 2

  constructor(private response: RawDeliverResponseV2) {
    if (this.response.key !== DeliverResponseV2.key) {
      throw new Error(`Unable to create ${DeliverResponseV2.name} from data of type ${this.response.key}`)
    }
  }

  toBuffer(): Buffer {
    const bufferSize = 1024
    const bufferSizeParams = { maxSize: bufferSize }
    const dw = new BufferDataWriter(Buffer.alloc(bufferSize), 4, bufferSizeParams)
    dw.writeUInt16(DeliverResponseV2.key)
    dw.writeUInt16(2)
    dw.writeUInt8(this.response.subscriptionId)
    dw.writeUInt64(this.response.committedChunkId)
    dw.writePrefixSize()
    return dw.toBuffer()
  }

  get key() {
    return this.response.key
  }

  get correlationId(): number {
    return -1
  }

  get code(): number {
    return -1
  }

  get ok(): boolean {
    return true
  }

  get subscriptionId(): number {
    return this.response.subscriptionId
  }

  get committedChunkId(): bigint {
    return this.response.committedChunkId
  }

  get messages(): Message[] {
    return this.response.messages
  }
}
