import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export type CreditRequestParams = {
  subscriptionId: number
  credit: number
}

export class CreditRequest extends AbstractRequest {
  static readonly Key = 0x09
  readonly key = CreditRequest.Key
  static readonly Version = 1
  readonly responseKey = -1

  constructor(private params: CreditRequestParams) {
    super()
  }

  protected writeContent(writer: DataWriter): void {
    writer.writeUInt8(this.params.subscriptionId)
    writer.writeUInt16(this.params.credit)
  }
}
