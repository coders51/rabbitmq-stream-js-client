import { ConsumerUpdateQuery } from "../responses/consumer_update_query"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"
import { Offset } from "./subscribe_request"

export class ConsumerUpdateResponse extends AbstractRequest {
  readonly responseKey = ConsumerUpdateQuery.key
  static readonly Key = 0x801a
  static readonly Version = 1
  readonly key = ConsumerUpdateResponse.Key

  constructor(private params: { correlationId: number; responseCode: number; offset: Offset }) {
    super()
  }

  writeContent(b: DataWriter) {
    b.writeUInt32(this.params.correlationId)
    b.writeUInt16(this.params.responseCode)
    this.params.offset.write(b)
  }
}
