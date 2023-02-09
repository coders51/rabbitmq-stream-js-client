import { inspect } from "util"
import { Logger } from "winston"
import { DecoderListenerFunc } from "./decoder_listener"
import { AbstractTypeClass } from "./responses/abstract_response"
import { DeclarePublisherResponse } from "./responses/declare_publisher_response"
import { CreateStreamResponse } from "./responses/create_stream_response"
import { HeartbeatResponse } from "./responses/heartbeat_response"
import { OpenResponse } from "./responses/open_response"
import { PeerPropertiesResponse } from "./responses/peer_properties_response"
import {
  DataReader,
  RawDeliverResponse,
  RawCreditResponse,
  RawHeartbeatResponse,
  RawMetadataUpdateResponse,
  RawResponse,
  RawTuneResponse,
} from "./responses/raw_response"
import { SaslAuthenticateResponse } from "./responses/sasl_authenticate_response"
import { SaslHandshakeResponse } from "./responses/sasl_handshake_response"
import { TuneResponse } from "./responses/tune_response"
import { DeleteStreamResponse } from "./responses/delete_stream_response"
import { CloseResponse } from "./responses/close_response"
import { QueryPublisherResponse } from "./responses/query_publisher_response"
import { MetadataUpdateResponse } from "./responses/metadata_update_response"
import { EventEmitter } from "events"
import { SubscribeResponse } from "./responses/subscribe_response"
import { DeliverResponse } from "./responses/deliver_response"
import { FormatCodeType, FormatCode } from "./amqp10/decoder"
import { CreditResponse } from "./responses/credit_response"

// Frame => Size (Request | Response | Command)
//   Size => uint32 (size without the 4 bytes of the size element)
//
// Response => Key Version CorrelationId ResponseCode
//   Key => uint16
//   Version => uint16
//   CorrelationId => uint32
//   ResponseCode => uint16

export type MetadataUpdateListener = (metadata: MetadataUpdateResponse) => void
export type CreditListener = (creditResponse: CreditResponse) => void
export type DeliverListener = (response: DeliverResponse) => void
type MessageAndSubId = {
  subscriptionId: number
  messages: Buffer[]
}

function decode(
  data: DataReader,
  logger: Logger
):
  | RawResponse
  | RawTuneResponse
  | RawHeartbeatResponse
  | RawMetadataUpdateResponse
  | RawDeliverResponse
  | RawCreditResponse {
  const size = data.readUInt32()
  return decodeResponse(data.readTo(size), size, logger)
}

function decodeResponse(
  dataResponse: DataReader,
  size: number,
  logger: Logger
):
  | RawResponse
  | RawTuneResponse
  | RawHeartbeatResponse
  | RawMetadataUpdateResponse
  | RawDeliverResponse
  | RawCreditResponse {
  const key = dataResponse.readUInt16()
  const version = dataResponse.readUInt16()
  if (key === DeliverResponse.key) {
    const { subscriptionId, messages } = decodeDeliverResponse(dataResponse, logger)
    const response: RawDeliverResponse = {
      size,
      key: key as DeliverResponse["key"],
      version,
      subscriptionId,
      messages,
    }
    return response
  }
  if (key === TuneResponse.key) {
    const frameMax = dataResponse.readUInt32()
    const heartbeat = dataResponse.readUInt32()
    return { size, key, version, frameMax, heartbeat } as RawTuneResponse
  }
  if (key === HeartbeatResponse.key) {
    return { key, version } as RawHeartbeatResponse
  }
  if (key === MetadataUpdateResponse.key) {
    const metadataInfo = {
      code: dataResponse.readUInt16(),
      stream: dataResponse.readString(),
    }
    return { size, key, version, metadataInfo } as RawMetadataUpdateResponse
  }
  if (key === CreditResponse.key) {
    const responseCodeCredit = dataResponse.readUInt16()
    const subscriptionId = dataResponse.readUInt8()
    return { size, key, version, responseCode: responseCodeCredit, subscriptionId: subscriptionId } as RawCreditResponse
  }
  const correlationId = dataResponse.readUInt32()
  const responseCode = dataResponse.readUInt16()
  const payload = dataResponse.readToEnd()
  return { size, key, version, correlationId, code: responseCode, payload }
}

function decodeDeliverResponse(dataResponse: DataReader, logger: Logger): MessageAndSubId {
  const subscriptionId = dataResponse.readUInt8()
  const magicVersion = dataResponse.readInt8()
  const chunkType = dataResponse.readInt8()
  const numEntries = dataResponse.readUInt16()
  const numRecords = dataResponse.readUInt32()
  const timestamp = dataResponse.readInt64()
  const epoch = dataResponse.readUInt64()
  const chunkFirstOffset = dataResponse.readUInt64()
  const chunkCrc = dataResponse.readInt32()
  const dataLength = dataResponse.readUInt32()
  const trailerLength = dataResponse.readUInt32()
  const reserved = dataResponse.readUInt32()
  const messageType = dataResponse.readUInt8()
  dataResponse.rewind(1)
  const messageLength = dataResponse.readUInt32()
  const formatCode = readFormatCodeType(dataResponse)

  const data = {
    magicVersion,
    chunkType,
    numEntries,
    numRecords,
    timestamp,
    epoch,
    chunkFirstOffset,
    chunkCrc,
    dataLength,
    trailerLength,
    reserved,
    messageType,
    messageLength,
  }
  logger.debug(inspect(data))

  const messages: Buffer[] = []

  for (let i = 0; i < numEntries; i++) {
    switch (formatCode) {
      case FormatCodeType.ApplicationData:
        const type = dataResponse.readUInt8()
        switch (type) {
          case FormatCode.Vbin8:
            const length = dataResponse.readUInt8()
            messages.push(dataResponse.readBufferOf(length))
        }
        break
      default:
        break
    }
  }
  return { subscriptionId, messages }
}

function readFormatCodeType(dataResponse: DataReader) {
  dataResponse.readInt8()
  dataResponse.readInt8()
  const formatCode = dataResponse.readInt8()
  return formatCode
}

export class BufferDataReader implements DataReader {
  private offset = 0

  constructor(private data: Buffer) {}

  readTo(size: number): DataReader {
    const ret = new BufferDataReader(this.data.slice(this.offset, this.offset + size))
    this.offset += size
    return ret
  }

  readBufferOf(size: number): Buffer {
    const ret = Buffer.from(this.data.slice(this.offset, this.offset + size))
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

  readInt8(): number {
    const ret = this.data.readInt8(this.offset)
    this.offset += 1
    return ret
  }

  readInt64(): bigint {
    const ret = this.data.readBigInt64BE(this.offset)
    this.offset += 8
    return ret
  }

  readUInt8(): number {
    const ret = this.data.readUInt8(this.offset)
    this.offset += 1
    return ret
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

  rewind(count: number): void {
    this.offset -= count
  }
}

function isTuneResponse(
  params:
    | RawResponse
    | RawTuneResponse
    | RawHeartbeatResponse
    | RawMetadataUpdateResponse
    | RawDeliverResponse
    | RawCreditResponse
): params is RawTuneResponse {
  return params.key === TuneResponse.key
}

function isHeartbeatResponse(
  params:
    | RawResponse
    | RawTuneResponse
    | RawHeartbeatResponse
    | RawMetadataUpdateResponse
    | RawDeliverResponse
    | RawCreditResponse
): params is RawHeartbeatResponse {
  return params.key === HeartbeatResponse.key
}

function isMetadataUpdateResponse(
  params:
    | RawResponse
    | RawTuneResponse
    | RawHeartbeatResponse
    | RawMetadataUpdateResponse
    | RawDeliverResponse
    | RawCreditResponse
): params is RawMetadataUpdateResponse {
  return params.key === MetadataUpdateResponse.key
}

function isDeliverResponse(
  params:
    | RawResponse
    | RawTuneResponse
    | RawHeartbeatResponse
    | RawMetadataUpdateResponse
    | RawDeliverResponse
    | RawCreditResponse
): params is RawDeliverResponse {
  return params.key === DeliverResponse.key
}

function isCreditResponse(
  params:
    | RawResponse
    | RawTuneResponse
    | RawHeartbeatResponse
    | RawMetadataUpdateResponse
    | RawCreditResponse
    | RawCreditResponse
): params is RawCreditResponse {
  return params.key === CreditResponse.key
}

export class ResponseDecoder {
  private responseFactories = new Map<number, AbstractTypeClass>()
  private emitter = new EventEmitter()

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
    this.addFactoryFor(SubscribeResponse)
  }

  add(data: Buffer) {
    const dataReader = new BufferDataReader(data)
    while (!dataReader.atEnd()) {
      const response = decode(dataReader, this.logger)
      if (isHeartbeatResponse(response)) {
        this.logger.debug(`heartbeat received from the server: ${inspect(response)}`)
      } else if (isTuneResponse(response)) {
        this.emitTuneResponseReceived(response)
        this.logger.debug(`tune received from the server: ${inspect(response)}`)
      } else if (isMetadataUpdateResponse(response)) {
        this.emitter.emit("metadata_update", new MetadataUpdateResponse(response))
        this.logger.debug(`metadata update received from the server: ${inspect(response)}`)
      } else if (isDeliverResponse(response)) {
        this.emitter.emit("deliver", new DeliverResponse(response))
        this.logger.debug(`deliver received from the server: ${inspect(response)}`)
      } else if (isCreditResponse(response)) {
        this.logger.debug(`credit received from the server: ${inspect(response)}`)
        this.emitter.emit("credit_response", new CreditResponse(response))
      } else {
        this.emitResponseReceived(response)
      }
    }
  }

  public on(
    event: "metadata_update" | "credit_response" | "deliver",
    listener: MetadataUpdateListener | CreditListener | DeliverListener
  ) {
    this.emitter.on(event, listener)
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
