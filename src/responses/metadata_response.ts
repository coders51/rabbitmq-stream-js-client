import { RawMetadataResponse } from "./raw_response"
import { Response } from "./response"
import { BufferDataWriter } from "../requests/abstract_request"

export class MetadataResponse implements Response {
  static key = 0x800f

  constructor(private response: RawMetadataResponse) {
    if (this.response.key !== MetadataResponse.key) {
      throw new Error(`Unable to create ${MetadataResponse.name} from data of type ${this.response.key}`)
    }
  }

  toBuffer(): Buffer {
    const dw = new BufferDataWriter(Buffer.alloc(1024), 4)
    dw.writeUInt16(MetadataResponse.key)
    dw.writeUInt16(1)
    dw.writeUInt32(this.response.correlationId)
    dw.writeUInt16(this.response.broker.reference)
    dw.writeString(this.response.broker.host)
    dw.writeUInt32(this.response.broker.port)
    dw.writeString(this.response.streamMetadata.streamName)
    dw.writeUInt16(this.response.streamMetadata.responseCode)
    dw.writeUInt16(this.response.streamMetadata.leaderReference)
    const lenRefs = this.response.streamMetadata.replicasReferences.length
    for (let i = 0; i < lenRefs; i++) {
      dw.writeUInt16(this.response.streamMetadata.replicasReferences[i])
    }
    return dw.toBuffer()
  }

  get key() {
    return this.response.key
  }

  get correlationId(): number {
    return this.response.correlationId
  }

  get code(): number {
    return this.response.streamMetadata.responseCode
  }

  get ok(): boolean {
    return true
  }

  get payload() {
    return this.response.payload
  }

  get metadata() {
    return this.response.streamMetadata
  }
}
