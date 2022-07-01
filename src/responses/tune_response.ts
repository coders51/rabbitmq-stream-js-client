import { BufferDataWriter } from "../requests/abstract_request"
import { RawTuneResponse } from "./raw_response"
import { Response } from "./response"

export class TuneResponse implements Response {
  static key = 0x0014 // I know it isn't 8014
  constructor(private response: RawTuneResponse) {
    if (this.response.key !== TuneResponse.key) {
      throw new Error(`Unable to create ${TuneResponse.name} from data of type ${this.response.key}`)
    }
  }

  toBuffer(): Buffer {
    const dw = new BufferDataWriter(Buffer.alloc(1024), 4)
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
}
