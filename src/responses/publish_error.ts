import { BufferDataWriter } from "../requests/abstract_request"
import { PublishingError, RawPublishError } from "./raw_response"

export class PublishError {
  static key = 0x0004 as const

  constructor(private response: RawPublishError) {
    if (this.response.key !== PublishError.key) {
      throw new Error(`Unable to create ${PublishError.name} from data of type ${this.response.key}`)
    }
  }

  toBuffer(): Buffer {
    const dw = new BufferDataWriter(Buffer.alloc(1024), 4)
    dw.writeUInt16(PublishError.key)
    dw.writeUInt16(1)
    dw.writeUInt8(this.response.publisherId)
    dw.writeUInt64(this.response.publishingError.publishingId)
    dw.writeUInt16(this.response.publishingError.code)
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

  get publishingError(): PublishingError {
    return this.response.publishingError
  }
}
