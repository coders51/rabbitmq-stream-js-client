/* eslint-disable no-param-reassign */
import { SaslAuthenticateResponse } from "../responses/sasl_authenticate_response"
import { AbstractRequest, writeString } from "./abstract_request"

export class SaslAuthenticateRequest extends AbstractRequest {
  readonly responseKey = SaslAuthenticateResponse.key
  readonly key = 0x0013

  constructor(private params: { mechanism: string; username: string; password: string }) {
    super()
  }

  protected writeContent(b: Buffer, offset: number) {
    offset = writeString(b, offset, this.params.mechanism)

    offset = b.writeUInt32BE(this.params.password.length + this.params.username.length + 2, offset)
    offset = b.writeUInt8(0, offset)
    const uw = b.write(this.params.username, offset)
    offset += uw
    offset = b.writeUInt8(0, offset)
    const pw = b.write(this.params.username, offset)
    offset += pw
    return offset
  }
}
