import { BufferDataWriter } from "../requests/buffer_data_writer"
import { RawCreditResponse } from "./raw_response"
import { Response } from "./response"

export class CreditResponse implements Response {
  static key = 0x8009 as const
  static readonly Version = 1

  constructor(private response: RawCreditResponse) {
    if (this.response.key !== CreditResponse.key) {
      throw new Error(`Unable to create ${CreditResponse.name} from data of type ${this.response.key}`)
    }
  }

  toBuffer(): Buffer {
    const bufferSize = 1024
    const bufferSizeParams = { maxSize: bufferSize }
    const dw = new BufferDataWriter(Buffer.alloc(bufferSize), 4, bufferSizeParams)
    dw.writeUInt16(CreditResponse.key)
    dw.writeUInt16(1)
    dw.writeUInt16(this.response.responseCode)
    dw.writeUInt8(this.response.subscriptionId)
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

  get responseCode(): number {
    return this.response.responseCode
  }

  get subscriptionId(): number {
    return this.response.subscriptionId
  }
}
