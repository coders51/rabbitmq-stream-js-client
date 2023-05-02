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
import { UnsubscribeResponse } from "./responses/unsubscribe_response"
import { Properties } from "./amqp10/properties"
import { Message, MessageProperties } from "./producer"
import { ApplicationProperties } from "./amqp10/applicationProperties"

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
  messages: Message[]
}

type PossibleRawResponses =
  | RawResponse
  | RawTuneResponse
  | RawHeartbeatResponse
  | RawMetadataUpdateResponse
  | RawDeliverResponse
  | RawCreditResponse

function decode(data: DataReader, logger: Logger): PossibleRawResponses {
  const size = data.readUInt32()
  return decodeResponse(data.readTo(size), size, logger)
}

function decodeResponse(dataResponse: DataReader, size: number, logger: Logger): PossibleRawResponses {
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
    const responseCode = dataResponse.readUInt16()
    const subscriptionId = dataResponse.readUInt8()
    const response: RawCreditResponse = {
      size,
      key,
      version,
      responseCode,
      subscriptionId,
    }
    return response
  }

  const correlationId = dataResponse.readUInt32()
  const code = dataResponse.readUInt16()
  const payload = dataResponse.readToEnd()
  return { size, key, version, correlationId, code, payload }
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

  const messages: Message[] = []

  let type
  let mapType
  let next
  let headerType
  let length
  let properties
  let applicationProperties
  let applicationPropertiesLength = 0

  for (let i = 0; i < numEntries; i++) {
    let content = Buffer.from("")
    let messageProperties: MessageProperties = {}

    switch (formatCode) {
      case FormatCodeType.ApplicationData:
        type = dataResponse.readUInt8()
        switch (type) {
          case FormatCode.Vbin8:
            const lengthVbin8 = dataResponse.readUInt8()
            content = dataResponse.readBufferOf(lengthVbin8)
        }
        break
      case FormatCodeType.MessageProperties:
        dataResponse.rewind(3)
        type = dataResponse.readInt8()

        if (type !== 0) {
          throw new Error(`invalid composite header %#02x: ${type}`)
        }

        const nextType = dataResponse.readInt8()
        switch (nextType) {
          case FormatCode.SmallUlong:
            const retSmallLong = dataResponse.readInt8()
            next = BigInt(retSmallLong)
            break
          case FormatCode.ULong:
            const retULong = dataResponse.readUInt64()
            next = retULong
            break
          default:
            next = 0n
        }
        headerType = dataResponse.readUInt8()

        switch (headerType) {
          case FormatCode.List0:
            length = 0
            break
          case FormatCode.List8:
            dataResponse.forward(1)
            const lenB = dataResponse.readInt8()
            length = lenB
            break
          case FormatCode.List32:
            dataResponse.forward(4)
            const lenI = dataResponse.readInt32()
            length = lenI
            messageProperties = Properties.Parse(dataResponse, length)
            break
          // default:
          //   throw new Error(`ReadCompositeHeader Invalid type ${headerType}`)
        }
        break
      case FormatCodeType.ApplicationProperties:
        mapType = dataResponse.readUInt8()
        switch (mapType) {
          case FormatCode.Map8:
            // Read first empty byte
            dataResponse.readUInt8()
            const shortNumElements = dataResponse.readUInt8()
            applicationPropertiesLength = shortNumElements
            break
          case FormatCode.Map32:
            // Read first empty four bytes
            dataResponse.readUInt32()
            const longNumElements = dataResponse.readUInt32()
            applicationPropertiesLength = longNumElements
            break
        }
        applicationProperties = ApplicationProperties.Parse(dataResponse, applicationPropertiesLength)
        break
      default:
        break
    }
    messages.push({ content, properties: messageProperties, applicationProperties })
  }

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
    formatCode,
    type,
    next,
    headerType,
    length,
    properties,
    applicationProperties,
  }
  logger.debug(inspect(data))
  const decodedMessages: Buffer[] = []
  for (let i = 0; i < numEntries; i++) {
    decodedMessages.push(decodeMessage(dataResponse))
  }

  return { subscriptionId, messages }
}

const EmptyBuffer = Buffer.from("")

function decodeMessage(dataResponse: DataReader): Buffer {
  const messageLength = dataResponse.readUInt32()
  const startFrom = dataResponse.position()

  let content = EmptyBuffer
  while (dataResponse.position() - startFrom !== messageLength) {
    const formatCode = readFormatCodeType(dataResponse)
    switch (formatCode) {
      case FormatCodeType.ApplicationData:
        content = decodeApplicationData(dataResponse)
        break
      case FormatCodeType.MessageAnnotations:
      case FormatCodeType.MessageProperties:
      case FormatCodeType.ApplicationProperties:
      case FormatCodeType.MessageHeader:
      case FormatCodeType.AmqpValue:
        throw new Error("Not implemented")
        break
      default:
        throw new Error(`Not supported format code ${formatCode}`)
    }
  }

  return content
}

function decodeApplicationData(dataResponse: DataReader) {
  const type = dataResponse.readUInt8()
  switch (type) {
    case FormatCode.Vbin8:
      const l8 = dataResponse.readUInt8()
      return dataResponse.readBufferOf(l8)
    case FormatCode.Vbin32:
      const l32 = dataResponse.readUInt32()
      return dataResponse.readBufferOf(l32)
    default:
      throw new Error(`Unknown data type ${type}`)
  }
}

function readFormatCodeType(dataResponse: DataReader) {
  dataResponse.readInt8()
  dataResponse.readInt8()
  return dataResponse.readInt8()
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

  readDouble(): number {
    const ret = this.data.readDoubleBE(this.offset)
    this.offset += 8
    return ret
  }

  readFloat(): number {
    const ret = this.data.readFloatBE(this.offset)
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

  readUTF8String(): string {
    // Reading of string type
    const type = this.readUInt8()
    switch (type) {
      case FormatCode.Str8:
      case FormatCode.Sym8:
        const sizeStr8 = this.readUInt8()
        const valueStr8 = this.data.toString("utf8", this.offset, this.offset + sizeStr8)
        this.offset += sizeStr8
        return valueStr8
      case FormatCode.Str32:
        const sizeStr32 = this.readUInt32()
        const valueStr32 = this.data.toString("utf8", this.offset, this.offset + sizeStr32)
        this.offset += sizeStr32
        return valueStr32
      default:
        throw new Error("ReadUTFString ERROR, unknown string type")
    }
  }

  rewind(count: number): void {
    this.offset -= count
  }

  forward(count: number): void {
    this.offset += count
  }
}

function isTuneResponse(params: PossibleRawResponses): params is RawTuneResponse {
  return params.key === TuneResponse.key
}

function isHeartbeatResponse(params: PossibleRawResponses): params is RawHeartbeatResponse {
  return params.key === HeartbeatResponse.key
}

function isMetadataUpdateResponse(params: PossibleRawResponses): params is RawMetadataUpdateResponse {
  return params.key === MetadataUpdateResponse.key
}

function isDeliverResponse(params: PossibleRawResponses): params is RawDeliverResponse {
  return params.key === DeliverResponse.key
}

function isCreditResponse(params: PossibleRawResponses): params is RawCreditResponse {
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
    this.addFactoryFor(UnsubscribeResponse)
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
