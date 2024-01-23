import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export type CreditRequestParams = {
  subscriptionId: number
  credit: number
}

export class CreditRequest extends AbstractRequest {
  static readonly Key = 0x09
  static readonly Version = 1

  constructor(private params: CreditRequestParams) {
    super()
  }

  protected writeContent(writer: DataWriter): void {
    writer.writeUInt8(this.params.subscriptionId)
    writer.writeUInt16(this.params.credit)
  }

  get key(): number {
    return CreditRequest.Key
  }
  get responseKey(): number {
    return -1
  }
  get version(): number {
    return CreditRequest.Version
  }
}
