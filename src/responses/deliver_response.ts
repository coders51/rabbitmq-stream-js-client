import { Message } from "../producer"
import { BufferDataWriter } from "../requests/abstract_request"
import { RawDeliverResponse } from "./raw_response"
import { Response } from "./response"

export class DeliverResponse implements Response {
  static key = 0x0008
  static readonly Version = 1

  constructor(private response: RawDeliverResponse) {
    if (this.response.key !== DeliverResponse.key) {
      throw new Error(`Unable to create ${DeliverResponse.name} from data of type ${this.response.key}`)
    }
  }

  toBuffer(): Buffer {
    const bufferSize = 1024
    const bufferSizeParams = { maxSize: bufferSize }
    const dw = new BufferDataWriter(Buffer.alloc(bufferSize), 4, bufferSizeParams)
    dw.writeUInt16(DeliverResponse.key)
    dw.writeUInt16(1)
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

  get subscriptionId(): number {
    return this.response.subscriptionId
  }

  get messages(): Message[] {
    return this.response.messages
  }
}
