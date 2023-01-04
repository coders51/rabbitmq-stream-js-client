import { inspect } from "util"
import { Logger } from "winston"
import { DecoderListenerFunc } from "./decoder_listener"
import { AbstractTypeClass } from "./responses/abstract_response"
import { DeclarePublisherResponse } from "./responses/declare_publisher_response"
import { CreateStreamResponse } from "./responses/create_stream_response"
import { HeartbeatResponse } from "./responses/heartbeat_response"
import { OpenResponse } from "./responses/open_response"
import { PeerPropertiesResponse } from "./responses/peer_properties_response"
import { DataReader, RawHeartbeatResponse, RawResponse, RawTuneResponse } from "./responses/raw_response"
import { SaslAuthenticateResponse } from "./responses/sasl_authenticate_response"
import { SaslHandshakeResponse } from "./responses/sasl_handshake_response"
import { TuneResponse } from "./responses/tune_response"
import { DeleteStreamResponse } from "./responses/delete_stream_response"
import { CloseResponse } from "./responses/close_response"
import { QueryPublisherResponse } from "./responses/query_publisher_response"

// Frame => Size (Request | Response | Command)
//   Size => uint32 (size without the 4 bytes of the size element)
//
// Response => Key Version CorrelationId ResponseCode
//   Key => uint16
//   Version => uint16
//   CorrelationId => uint32
//   ResponseCode => uint16

function decode(data: DataReader): RawResponse | RawTuneResponse | RawHeartbeatResponse {
  const size = data.readUInt32()
  return decodeResponse(data.readTo(size), size)
}

function decodeResponse(dataResponse: DataReader, size: number): RawResponse | RawTuneResponse | RawHeartbeatResponse {
  const key = dataResponse.readUInt16()
  const version = dataResponse.readUInt16()
  if (key === TuneResponse.key) {
    const frameMax = dataResponse.readUInt32()
    const heartbeat = dataResponse.readUInt32()
    return { size, key, version, frameMax, heartbeat } as RawTuneResponse
  }
  if (key === HeartbeatResponse.key) {
    return { key, version } as RawHeartbeatResponse
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

  readUInt64(): bigint {
    const ret = this.data.readBigUInt64BE(this.offset)
    this.offset += 8
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

function isTuneResponse(params: RawResponse | RawTuneResponse | RawHeartbeatResponse): params is RawTuneResponse {
  return params.key === TuneResponse.key
}

function isHeartbeatResponse(
  params: RawResponse | RawTuneResponse | RawHeartbeatResponse
): params is RawHeartbeatResponse {
  return params.key === HeartbeatResponse.key
}

export class ResponseDecoder {
  private responseFactories = new Map<number, AbstractTypeClass>()

  constructor(private listener: DecoderListenerFunc, private logger: Logger) {
    this.addFactoryFor(PeerPropertiesResponse)
    this.addFactoryFor(SaslHandshakeResponse)
    this.addFactoryFor(SaslAuthenticateResponse)
    this.addFactoryFor(OpenResponse)
    this.addFactoryFor(CloseResponse)
    this.addFactoryFor(DeclarePublisherResponse)
    this.addFactoryFor(CreateStreamResponse)
    this.addFactoryFor(DeleteStreamResponse)
    this.addFactoryFor(QueryPublisherResponse)
  }

  add(data: Buffer) {
    const dataReader = new BufferDataReader(data)
    while (!dataReader.atEnd()) {
      const response = decode(dataReader)
      if (isTuneResponse(response)) {
        this.emitTuneResponseReceived(response)
      } else if (isHeartbeatResponse(response)) {
        this.logger.debug(`heartbeat received from the server: ${inspect(response)}`)
      } else {
        this.emitResponseReceived(response)
      }
    }
  }

  private addFactoryFor(type: AbstractTypeClass) {
    this.responseFactories.set(type.key, type)
  }

  private emitTuneResponseReceived(response: RawTuneResponse) {
    this.listener(new TuneResponse(response))
  }

  private emitResponseReceived(response: RawResponse) {
    const value = this.getFactoryFor(response.key)
    // TODO: this if should be removed when we have implemented the publish confirm
    if (!value) return
    this.listener(new value(response))
  }

  private getFactoryFor(key: number): AbstractTypeClass | undefined {
    const value = this.responseFactories.get(key)
    // TODO: this undefined and verify of 3 should be removed when we have implemented the publish confirm command
    if (!value && key !== 3) {
      throw new Error(`Unknown response ${key.toString(16)}`)
    }
    return value
  }
}
