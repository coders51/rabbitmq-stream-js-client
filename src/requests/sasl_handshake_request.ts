import { SaslHandshakeResponse } from "../responses/sasl_handshake_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class SaslHandshakeRequest extends AbstractRequest {
  static readonly Key = 0x0012
  static readonly Version = 1

  protected writeContent(_dw: DataWriter) {
    // do nothing
  }

  get key(): number {
    return SaslHandshakeRequest.Key
  }
  get responseKey(): number {
    return SaslHandshakeResponse.key
  }
  get version(): number {
    return SaslHandshakeRequest.Version
  }
}
