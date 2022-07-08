import { DecoderListener } from "./decoder_listener"
import { AbstractTypeClass } from "./responses/abstract_response"
import { OpenResponse } from "./responses/open_response"
import { PeerPropertiesResponse } from "./responses/peer_properties_response"
import { DataReader, RawResponse, RawTuneResponse } from "./responses/raw_response"
import { SaslAuthenticateResponse } from "./responses/sasl_authenticate_response"
import { SaslHandshakeResponse } from "./responses/sasl_handshake_response"
import { TuneResponse } from "./responses/tune_response"

// Frame => Size (Request | Response | Command)
//   Size => uint32 (size without the 4 bytes of the size element)
//
// Response => Key Version CorrelationId ResponseCode
//   Key => uint16
//   Version => uint16
//   CorrelationId => uint32
//   ResponseCode => uint16

function decode(data: DataReader): RawResponse | RawTuneResponse {
  const size = data.readUInt32()
  return decodeResponse(data.readTo(size), size)
}

function decodeResponse(dataResponse: DataReader, size: number): RawResponse | RawTuneResponse {
  const key = dataResponse.readUInt16()
  const version = dataResponse.readUInt16()
  if (key === TuneResponse.key) {
    const frameMax = dataResponse.readUInt32()
    const heartbeat = dataResponse.readUInt32()
    return { size, key, version, frameMax, heartbeat } as RawTuneResponse
  }
  const correlationId = dataResponse.readUInt32()
  const responseCode = dataResponse.readUInt16()
  const payload = dataResponse.readToEnd()
  return { size, key, version, correlationId, code: responseCode, payload }
}

class BufferDataReader implements DataReader {
  private offset = 0

  constructor(private data: Buffer) {}

  readTo(size: number): DataReader {
    const ret = new BufferDataReader(this.data.slice(this.offset, this.offset + size))
    this.offset += size
    return ret
  }

  readToEnd(): DataReader {
    const ret = new BufferDataReader(this.data.slice(this.offset))
    this.offset = this.data.length
    return ret
  }

  atEnd(): boolean {
    return this.offset === this.data.length
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

function isTuneResponse(params: RawResponse | RawTuneResponse): params is RawTuneResponse {
  return params.key === TuneResponse.key
}

export class ResponseDecoder {
  private responseFactories = new Map<number, AbstractTypeClass>()

  constructor(private listener: DecoderListener) {
    this.addFactoryFor(PeerPropertiesResponse)
    this.addFactoryFor(SaslHandshakeResponse)
    this.addFactoryFor(SaslAuthenticateResponse)
    this.addFactoryFor(OpenResponse)
  }

  add(data: Buffer) {
    const dataReader = new BufferDataReader(data)
    while (!dataReader.atEnd()) {
      const response = decode(dataReader)
      if (isTuneResponse(response)) {
        this.emitTuneResponseReceived(response)
      } else {
        this.emitResponseReceived(response)
      }
    }
  }

  private addFactoryFor(type: AbstractTypeClass) {
    this.responseFactories.set(type.key, type)
  }

  private emitTuneResponseReceived(response: RawTuneResponse) {
    this.listener.responseReceived(new TuneResponse(response))
  }

  private emitResponseReceived(response: RawResponse) {
    const value = this.getFactoryFor(response.key)
    this.listener.responseReceived(new value(response))
  }

  private getFactoryFor(key: number): AbstractTypeClass {
    const value = this.responseFactories.get(key)
    if (!value) {
      throw new Error(`Unknown response ${key.toString(16)}`)
    }
    return value
  }
}
