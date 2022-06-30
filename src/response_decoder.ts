import { DecoderListener } from "./decoder_listener"
import { OpenResponse } from "./responses/open_response"
import { PeerPropertiesResponse } from "./responses/peer_properties_response"
import { DataReader, RawResponse } from "./responses/raw_response"
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

function decode(data: DataReader): RawResponse {
  const size = data.readUInt32()
  const key = data.readUInt16()
  const version = data.readUInt16()
  const correlationId = data.readUInt32()
  const responseCode = data.readUInt16()
  const payload = data.slice()
  return { size, key, version, correlationId, code: responseCode, payload }
}

class BufferDataReader implements DataReader {
  private offset = 0
  constructor(private data: Buffer) {}
  slice(): DataReader {
    return new BufferDataReader(this.data.slice(this.offset))
  }
  readUInt16(): number {
    const ret = this.data.readUInt16BE(this.offset)
    this.offset += 2
    return ret
  }

  readUInt32(): number {
    const ret = this.data.readUInt32BE(this.offset)
    this.offset += 4
    return ret
  }

  readInt32(): number {
    const ret = this.data.readInt32BE(this.offset)
    this.offset += 4
    return ret
  }

  readString(): string {
    const size = this.readUInt16()
    const value = this.data.toString("utf8", this.offset, this.offset + size)
    this.offset += size
    return value
  }
}

export class ResponseDecoder {
  constructor(private listener: DecoderListener) {}

  add(data: Buffer) {
    const response = decode(new BufferDataReader(data))
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
