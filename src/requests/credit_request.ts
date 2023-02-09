import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

type CreditRequestParams = {
  subscriptionId: number
  credit: number
}

export class CreditRequest extends AbstractRequest {
  readonly key = 0x09
  readonly responseKey = -1

  constructor(private params: CreditRequestParams) {
    super()
  }

  protected writeContent(writer: DataWriter): void {
    writer.writeUInt8(this.params.subscriptionId)
    writer.writeUInt16(this.params.credit)
  }
}
