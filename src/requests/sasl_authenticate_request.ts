import { SaslAuthenticateResponse } from "../responses/sasl_authenticate_response"
import { AbstractRequest } from "./abstract_request"

export interface DataWriter {
  writeData(data: string): void
  writeUInt8(data: number): void
  writeUInt32(data: number): void
  writeString(data: string): void
}

export class SaslAuthenticateRequest extends AbstractRequest {
  readonly responseKey = SaslAuthenticateResponse.key
  readonly key = 0x0013

  constructor(private params: { mechanism: string; username: string; password: string }) {
    super()
  }

  protected writeContent(b: DataWriter): void {
    b.writeString(this.params.mechanism)
    b.writeUInt32(this.params.password.length + this.params.username.length + 2)
    b.writeUInt8(0)
    b.writeData(this.params.username)
    b.writeUInt8(0)
    b.writeData(this.params.username)
  }
}
