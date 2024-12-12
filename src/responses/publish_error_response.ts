import { BufferDataWriter } from "../requests/buffer_data_writer"
import { RawPublishErrorResponse } from "./raw_response"
import { Response } from "./response"

interface PublishingError {
  publishingId: bigint
  code: number
}

export class PublishErrorResponse implements Response {
  static key = 0x0004
  static readonly Version = 1

  readonly publisherId: number
  public publishingError: PublishingError
  constructor(private response: RawPublishErrorResponse) {
    if (this.response.key !== PublishErrorResponse.key) {
      throw new Error(`Unable to create ${PublishErrorResponse.name} from data of type ${this.response.key}`)
    }
    this.publishingError = { publishingId: response.publishingId, code: response.code }
    this.publisherId = response.publisherId
  }

  toBuffer(): Buffer {
    const bufferSize = 1024
    const bufferSizeParams = { maxSize: bufferSize }
    const dw = new BufferDataWriter(Buffer.alloc(bufferSize), 4, bufferSizeParams)
    dw.writeUInt16(PublishErrorResponse.key)
    dw.writeUInt16(1)
    dw.writeUInt8(this.publisherId)
    dw.writeUInt64(this.publishingError.publishingId)
    dw.writeUInt16(this.publishingError.code)
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
}
