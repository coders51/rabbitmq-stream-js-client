import { BufferDataWriter } from "../requests/buffer_data_writer"
import { MetadataInfo, RawMetadataUpdateResponse } from "./raw_response"
import { Response } from "./response"

export class MetadataUpdateResponse implements Response {
  static key = 0x0010
  static readonly Version = 1

  constructor(private response: RawMetadataUpdateResponse) {
    if (this.response.key !== MetadataUpdateResponse.key) {
      throw new Error(`Unable to create ${MetadataUpdateResponse.name} from data of type ${this.response.key}`)
    }
  }

  toBuffer(): Buffer {
    const bufferSize = 1024
    const bufferSizeParams = { maxSize: bufferSize }
    const dw = new BufferDataWriter(Buffer.alloc(bufferSize), 4, bufferSizeParams)
    dw.writeUInt16(MetadataUpdateResponse.key)
    dw.writeUInt16(1)
    dw.writeUInt16(this.response.metadataInfo.code)
    dw.writeString(this.response.metadataInfo.stream)
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

  get metadataInfo(): MetadataInfo {
    return this.response.metadataInfo
  }
}
