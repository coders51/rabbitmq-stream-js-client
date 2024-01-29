import { BufferDataWriter } from "../requests/buffer_data_writer"
import { RawTuneResponse } from "./raw_response"
import { Response } from "./response"

export class TuneResponse implements Response {
  static key = 0x0014 // I know it isn't 8014
  static readonly Version = 1

  constructor(private response: RawTuneResponse) {
    if (this.response.key !== TuneResponse.key) {
      throw new Error(`Unable to create ${TuneResponse.name} from data of type ${this.response.key}`)
    }
  }

  toBuffer(): Buffer {
    const bufferSize = 1024
    const bufferSizeParams = { maxSize: bufferSize }
    const dw = new BufferDataWriter(Buffer.alloc(bufferSize), 4, bufferSizeParams)
    dw.writeUInt16(TuneResponse.key)
    dw.writeUInt16(1)
    dw.writeUInt32(this.response.frameMax)
    dw.writeUInt32(this.response.heartbeat)
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

  get frameMax(): number {
    return this.response.frameMax
  }

  get heartbeat(): number {
    return this.response.heartbeat
  }
}
