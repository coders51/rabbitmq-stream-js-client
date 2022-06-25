import { SaslHandshakeResponse } from "../responses/sasl_handshake_response"
import { AbstractRequest } from "./abstract_request"

export class SaslHandshakeRequest extends AbstractRequest {
  readonly responseKey = SaslHandshakeResponse.key
  readonly key = 0x0012

  protected writeContent(_b: Buffer, offset: number): number {
    return offset
  }
}
