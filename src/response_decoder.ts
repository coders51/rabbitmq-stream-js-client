import { DecoderListener } from "./decoder_listener"
import { OpenResponse } from "./responses/open_response"
import { PeerPropertiesResponse } from "./responses/peer_properties_response"
import { RawResponse } from "./responses/raw_response"
import { SaslAuthenticateResponse } from "./responses/sasl_authenticate_response"
import { SaslHandshakeResponse } from "./responses/sasl_handshake_response"

// Frame => Size (Request | Response | Command)
//   Size => uint32 (size without the 4 bytes of the size element)
//
// Response => Key Version CorrelationId ResponseCode
//   Key => uint16
//   Version => uint16
//   CorrelationId => uint32
//   ResponseCode => uint16

function decode(data: Buffer): RawResponse {
  let offset = 0
  const size = data.readUint32BE(offset)
  offset += 4
  const key = data.readUint16BE(offset)
  offset += 2
  const version = data.readUint16BE(offset)
  offset += 2
  const correlationId = data.readUint32BE(offset)
  offset += 4
  const responseCode = data.readUint16BE(offset)
  offset += 2
  const payload = data.slice(offset)
  return { size, key, version, correlationId, code: responseCode, payload }
}

export class ResponseDecoder {
  constructor(private listener: DecoderListener) {}

  add(data: Buffer) {
    const response = decode(data)
    switch (response.key) {
      case PeerPropertiesResponse.key:
        this.listener.responseReceived(new PeerPropertiesResponse(response))
        break

      case SaslHandshakeResponse.key:
        this.listener.responseReceived(new SaslHandshakeResponse(response))
        break

      case SaslAuthenticateResponse.key:
        this.listener.responseReceived(new SaslAuthenticateResponse(response))
        break

      case OpenResponse.key:
        this.listener.responseReceived(new OpenResponse(response))
        break

      default:
        throw new Error(`Unknown response ${response.key.toString(16)}`)
    }
  }
}
