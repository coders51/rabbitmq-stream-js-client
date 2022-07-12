import { SaslHandshakeResponse } from "../responses/sasl_handshake_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class SaslHandshakeRequest extends AbstractRequest {
  readonly responseKey = SaslHandshakeResponse.key
  readonly key = 0x0012

  protected writeContent(_dw: DataWriter) {
    // do nothing
  }
}
