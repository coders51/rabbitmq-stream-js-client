import { ConsumerUpdateQueryResponse } from "../responses/consumer_update_query_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class ConsumerUpdateRequest extends AbstractRequest {
  readonly responseKey = ConsumerUpdateQueryResponse.key
  static readonly Key = 0x801a
  static readonly Version = 1
  readonly key = ConsumerUpdateRequest.Key

  constructor(private params: { correlationId: number; responseCode: number; offsetType: number; offset: bigint }) {
    super()
  }

  writeContent(b: DataWriter) {
    b.writeUInt32(this.params.correlationId)
    b.writeUInt16(this.params.responseCode)
    b.writeUInt16(this.params.offsetType)
    if (this.params.offsetType === 5) {
      b.writeInt64(this.params.offset)
      return
    }
    b.writeUInt64(this.params.offset)
  }
}
