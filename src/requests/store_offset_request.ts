import { StoreOffsetResponse } from "../responses/store_offset_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class StoreOffsetRequest extends AbstractRequest {
  static readonly Key = 0x000a
  static readonly Version = 1
  private readonly reference: string
  private readonly stream: string
  private readonly offsetValue: bigint

  constructor(params: { reference: string; stream: string; offsetValue: bigint }) {
    super()
    this.stream = params.stream
    this.reference = params.reference
    this.offsetValue = params.offsetValue
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.reference)
    writer.writeString(this.stream)
    writer.writeUInt64(this.offsetValue)
  }

  get key(): number {
    return StoreOffsetRequest.Key
  }
  get responseKey(): number {
    return StoreOffsetResponse.key
  }
  get version(): number {
    return StoreOffsetRequest.Version
  }
}
