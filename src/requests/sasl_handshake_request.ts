import { SaslHandshakeResponse } from "../responses/sasl_handshake_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class SaslHandshakeRequest extends AbstractRequest {
  readonly responseKey = SaslHandshakeResponse.key
  static readonly Key = 0x0012
  static readonly Version = 1
  readonly key = SaslHandshakeRequest.Key

  protected writeContent(_dw: DataWriter) {
    // do nothing
  }
}
