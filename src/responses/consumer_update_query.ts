import { BufferDataWriter } from "../requests/buffer_data_writer"
import { RawConsumerUpdateQueryResponse as RawConsumerUpdateQuery } from "./raw_response"
import { Response } from "./response"

export class ConsumerUpdateQuery implements Response {
  static key = 0x001a
  static readonly Version = 1

  constructor(private response: RawConsumerUpdateQuery) {
    if (this.response.key !== ConsumerUpdateQuery.key) {
      throw new Error(`Unable to create ${ConsumerUpdateQuery.name} from data of type ${this.response.key}`)
    }
  }

  toBuffer(): Buffer {
    const bufferSize = 1024
    const bufferSizeParams = { maxSize: bufferSize }
    const dw = new BufferDataWriter(Buffer.alloc(bufferSize), 4, bufferSizeParams)
    dw.writeUInt16(ConsumerUpdateQuery.key)
    dw.writeUInt16(1)
    dw.writeUInt32(this.response.correlationId)
    dw.writeUInt8(this.response.subscriptionId)
    dw.writeUInt8(this.response.active)
    dw.writePrefixSize()
    return dw.toBuffer()
  }

  get key() {
    return this.response.key
  }

  get correlationId(): number {
    return this.response.correlationId
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

  get active(): number {
    return this.response.active
  }
}
