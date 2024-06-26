import { EventEmitter } from "events"
import { inspect } from "util"
import { ApplicationProperties } from "./amqp10/applicationProperties"
import { FormatCode, FormatCodeType } from "./amqp10/decoder"
import { Annotations } from "./amqp10/messageAnnotations"
import { Header } from "./amqp10/messageHeader"
import { Properties } from "./amqp10/properties"
import { Compression, CompressionType } from "./compression"
import { DecoderListenerFunc } from "./decoder_listener"
import { Logger } from "./logger"
import {
  Message,
  MessageAnnotations,
  MessageApplicationProperties,
  MessageHeader,
  MessageProperties,
} from "./publisher"
import { AbstractTypeClass } from "./responses/abstract_response"
import { CloseResponse } from "./responses/close_response"
import { CreateStreamResponse } from "./responses/create_stream_response"
import { CreditResponse } from "./responses/credit_response"
import { DeclarePublisherResponse } from "./responses/declare_publisher_response"
import { DeletePublisherResponse } from "./responses/delete_publisher_response"
import { DeleteStreamResponse } from "./responses/delete_stream_response"
import { DeliverResponse } from "./responses/deliver_response"
import { HeartbeatResponse } from "./responses/heartbeat_response"
import { MetadataUpdateResponse } from "./responses/metadata_update_response"
import { OpenResponse } from "./responses/open_response"
import { PeerPropertiesResponse } from "./responses/peer_properties_response"
import { PublishConfirmResponse } from "./responses/publish_confirm_response"
import { PublishErrorResponse } from "./responses/publish_error_response"
import { QueryOffsetResponse } from "./responses/query_offset_response"
import { QueryPublisherResponse } from "./responses/query_publisher_response"
import {
  DataReader,
  RawConsumerUpdateQueryResponse as RawConsumerUpdateQuery,
  RawCreditResponse,
  RawDeliverResponse,
  RawDeliverResponseV2,
  RawHeartbeatResponse,
  RawMetadataUpdateResponse,
  RawPublishConfirmResponse,
  RawPublishErrorResponse,
  RawResponse,
  RawTuneResponse,
} from "./responses/raw_response"
import { SaslAuthenticateResponse } from "./responses/sasl_authenticate_response"
import { SaslHandshakeResponse } from "./responses/sasl_handshake_response"
import { StoreOffsetResponse } from "./responses/store_offset_response"
import { StreamStatsResponse } from "./responses/stream_stats_response"
import { SubscribeResponse } from "./responses/subscribe_response"
import { TuneResponse } from "./responses/tune_response"
import { UnsubscribeResponse } from "./responses/unsubscribe_response"
import { MetadataResponse } from "./responses/metadata_response"
import { ExchangeCommandVersionsResponse } from "./responses/exchange_command_versions_response"
import { RouteResponse } from "./responses/route_response"
import { PartitionsResponse } from "./responses/partitions_response"
import { ConsumerUpdateQuery } from "./responses/consumer_update_query"
import { CreateSuperStreamResponse } from "./responses/create_super_stream_response"
import { DeleteSuperStreamResponse } from "./responses/delete_super_stream_response"
import { DeliverResponseV2 } from "./responses/deliver_response_v2"

// Frame => Size (Request | Response | Command)
//   Size => uint32 (size without the 4 bytes of the size element)
//
// Response => Key Version CorrelationId ResponseCode
//   Key => uint16
//   Version => uint16
//   CorrelationId => uint32
//   ResponseCode => uint16
const UINT32_SIZE = 4

export type MetadataUpdateListener = (metadata: MetadataUpdateResponse) => void
export type CreditListener = (creditResponse: CreditResponse) => void
export type DeliverListener = (response: DeliverResponse) => void
export type DeliverV2Listener = (response: DeliverResponseV2) => void
export type PublishConfirmListener = (confirm: PublishConfirmResponse) => void
export type PublishErrorListener = (confirm: PublishErrorResponse) => void
export type ConsumerUpdateQueryListener = (metadata: ConsumerUpdateQuery) => void

type DeliveryResponseDecoded = {
  subscriptionId: number
  committedChunkId?: bigint
  messages: Message[]
}

type PossibleRawResponses =
  | RawResponse
  | RawTuneResponse
  | RawHeartbeatResponse
  | RawMetadataUpdateResponse
  | RawDeliverResponse
  | RawDeliverResponseV2
  | RawCreditResponse
  | RawPublishConfirmResponse
  | RawPublishErrorResponse
  | RawConsumerUpdateQuery

function decode(
  data: DataReader,
  getCompressionBy: (type: CompressionType) => Compression,
  logger: Logger
): { completed: true; response: PossibleRawResponses } | { completed: false; response: Buffer } {
  if (data.available() < UINT32_SIZE) return { completed: false, response: data.readBufferOf(data.available()) }

  const size = data.readUInt32()
  if (size > data.available()) {
    data.rewind(UINT32_SIZE)
    return { completed: false, response: data.readBufferOf(data.available()) }
  }

  return { completed: true, response: decodeResponse(data.readTo(size), size, getCompressionBy, logger) }
}

function decodeResponse(
  dataResponse: DataReader,
  size: number,
  getCompressionBy: (type: CompressionType) => Compression,
  logger: Logger
): PossibleRawResponses {
  const key = dataResponse.readUInt16()
  const version = dataResponse.readUInt16()
  if (key === DeliverResponse.key) {
    const { subscriptionId, committedChunkId, messages } = decodeDeliverResponse(
      dataResponse,
      getCompressionBy,
      logger,
      version
    )
    return {
      size,
      key: key as DeliverResponse["key"],
      version,
      subscriptionId,
      committedChunkId,
      messages,
    }
  }
  if (key === TuneResponse.key) {
    const frameMax = dataResponse.readUInt32()
    const heartbeat = dataResponse.readUInt32()
    return { size, key, version, frameMax, heartbeat } as RawTuneResponse
  }
  if (key === ConsumerUpdateQuery.key) {
    const correlationId = dataResponse.readUInt32()
    const subscriptionId = dataResponse.readUInt8()
    const active = dataResponse.readUInt8()
    const data = { size, key, version, correlationId, subscriptionId, active }
    logger.info(inspect(data))
    return data as RawConsumerUpdateQuery
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

  if (key === PublishConfirmResponse.key) {
    const publisherId = dataResponse.readUInt8()
    const publishingIds: bigint[] = []
    const howManyPublishingIds = dataResponse.readUInt32()
    for (let i = 0; i < howManyPublishingIds; i++) {
      const publishingId = dataResponse.readUInt64()
      publishingIds.push(publishingId)
    }
    const response: RawPublishConfirmResponse = {
      size,
      key: key as PublishConfirmResponse["key"],
      version,
      publisherId,
      publishingIds,
    }
    return response
  }
  const correlationId = dataResponse.readUInt32()
  const code = dataResponse.readUInt16()
  if (key === MetadataResponse.key) {
    // metadata response doesn't contain a code
    dataResponse.rewind(2)
  }
  const payload = dataResponse.readToEnd()
  return { size, key, version, correlationId, code, payload }
}

function decodeDeliverResponse(
  dataResponse: DataReader,
  getCompressionBy: (type: CompressionType) => Compression,
  logger: Logger,
  version = 1
): DeliveryResponseDecoded {
  const subscriptionId = dataResponse.readUInt8()
  const committedChunkId = version === 2 ? dataResponse.readUInt64() : undefined
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

  const messages: Message[] = []
  const data = {
    committedChunkId,
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
    messageType, // indicate if it contains subentries
  }
  logger.debug(inspect(data))

  if (messageType === 0) {
    dataResponse.rewind(1) // if not a sub entry, the messageType is part of the message
    for (let i = 0; i < numEntries; i++) {
      messages.push(decodeMessage(dataResponse, chunkFirstOffset + BigInt(i)))
    }
  } else {
    const compressionType = (messageType & 0x70) >> 4
    const compression = getCompressionBy(compressionType)
    messages.push(...decodeSubEntries(dataResponse, compression, logger))
  }

  return { subscriptionId, committedChunkId, messages }
}

const EmptyBuffer = Buffer.from("")

function decodeMessage(dataResponse: DataReader, offset: bigint): Message {
  const messageLength = dataResponse.readUInt32()
  const startFrom = dataResponse.position()

  let content = EmptyBuffer
  let messageAnnotations: MessageAnnotations = {}
  let messageProperties: MessageProperties = {}
  let messageHeader: MessageHeader = {}
  let amqpValue: string = ""
  let applicationProperties: MessageApplicationProperties = {}
  while (dataResponse.position() - startFrom !== messageLength) {
    const formatCode = readFormatCodeType(dataResponse)
    switch (formatCode) {
      case FormatCodeType.ApplicationData:
        content = decodeApplicationData(dataResponse)
        break
      case FormatCodeType.MessageAnnotations:
        messageAnnotations = decodeMessageAnnotations(dataResponse)
        break
      case FormatCodeType.MessageProperties:
        messageProperties = decodeMessageProperties(dataResponse)
        break
      case FormatCodeType.ApplicationProperties:
        applicationProperties = decodeApplicationProperties(dataResponse)
        break
      case FormatCodeType.MessageHeader:
        messageHeader = decodeMessageHeader(dataResponse)
        break
      case FormatCodeType.AmqpValue:
        amqpValue = decodeAmqpValue(dataResponse)
        break
      default:
        throw new Error(`Not supported format code ${formatCode}`)
    }
  }

  return { content, messageProperties, messageHeader, applicationProperties, amqpValue, messageAnnotations, offset }
}

function decodeSubEntries(dataResponse: DataReader, compression: Compression, logger: Logger): Message[] {
  const decodedMessages: Message[] = []
  const noOfRecords = dataResponse.readUInt16()
  const uncompressedLength = dataResponse.readUInt32()
  const compressedLength = dataResponse.readUInt32()
  const decompressedData = new BufferDataReader(compression.decompress(dataResponse.readBufferOf(compressedLength)))
  logger.debug(
    `Decoding sub entries, uncompressed length is ${uncompressedLength} while actual length is ${compressedLength}`
  )
  for (let i = 0; i < noOfRecords; i++) {
    const entry: Message = decodeMessage(decompressedData, BigInt(i))
    decodedMessages.push(entry)
  }
  return decodedMessages
}

function decodeApplicationProperties(dataResponse: DataReader) {
  const formatCode = dataResponse.readUInt8()
  const applicationPropertiesLength = decodeFormatCode(dataResponse, formatCode)

  return ApplicationProperties.parse(dataResponse, applicationPropertiesLength as number)
}

function decodeMessageAnnotations(dataResponse: DataReader) {
  const formatCode = dataResponse.readUInt8()
  const messageAnnotationsLength = decodeFormatCode(dataResponse, formatCode)

  return Annotations.parse(dataResponse, messageAnnotationsLength as number)
}

function decodeMessageProperties(dataResponse: DataReader) {
  dataResponse.rewind(3)
  const type = dataResponse.readInt8()
  if (type !== 0) {
    throw new Error(`invalid composite header: ${type}`)
  }

  const nextType = dataResponse.readInt8()
  decodeFormatCode(dataResponse, nextType)

  const formatCode = dataResponse.readUInt8()
  const propertiesLength = decodeFormatCode(dataResponse, formatCode)

  return Properties.parse(dataResponse, propertiesLength as number)
}

function decodeMessageHeader(dataResponse: DataReader) {
  dataResponse.rewind(3)
  const type = dataResponse.readInt8()
  if (type !== 0) {
    throw new Error(`invalid composite header: ${type}`)
  }

  // next, the composite type is encoded as an AMQP uint8
  dataResponse.readUInt64()

  const formatCode = dataResponse.readUInt8()
  const headerLength = decodeFormatCode(dataResponse, formatCode)

  return Header.parse(dataResponse, headerLength as number)
}

function decodeApplicationData(dataResponse: DataReader) {
  const formatCode = dataResponse.readUInt8()
  const length = decodeFormatCode(dataResponse, formatCode)

  return dataResponse.readBufferOf(length as number)
}

function decodeAmqpValue(dataResponse: DataReader) {
  const amqpFormatCode = dataResponse.readUInt8()
  dataResponse.rewind(1)
  return decodeFormatCode(dataResponse, amqpFormatCode, true) as string
}

function readFormatCodeType(dataResponse: DataReader) {
  dataResponse.readUInt8()
  dataResponse.readUInt8()

  return dataResponse.readUInt8()
}

export function readUTF8String(dataResponse: DataReader) {
  const formatCode = dataResponse.readUInt8()
  const decodedString = decodeFormatCode(dataResponse, formatCode)
  if (!decodedString) throw new Error(`invalid formatCode %#02x: ${formatCode}`)

  return decodedString as string
}

export function decodeBooleanType(dataResponse: DataReader, defaultValue: boolean) {
  const boolType = dataResponse.readInt8()
  switch (boolType) {
    case FormatCode.Bool:
      const boolValue = dataResponse.readInt8()
      return boolValue !== 0
    case FormatCode.BoolTrue:
      return true
    case FormatCode.BoolFalse:
      return false
    default:
      return defaultValue
  }
}

export function decodeFormatCode(dataResponse: DataReader, formatCode: number, skipByte = false) {
  switch (formatCode) {
    case FormatCode.Map8:
      // Read first empty byte
      dataResponse.readUInt8()
      return dataResponse.readUInt8()
    case FormatCode.Map32:
      // Read first empty four bytes
      dataResponse.readUInt32()
      return dataResponse.readUInt32()
    case FormatCode.SmallUlong:
      return dataResponse.readInt8() // Read a SmallUlong
    case FormatCode.Ubyte:
      dataResponse.forward(1)
      return dataResponse.readUInt8()
    case FormatCode.ULong:
      return dataResponse.readUInt64() // Read an ULong
    case FormatCode.List0:
      return 0
    case FormatCode.List8:
      dataResponse.forward(1)
      return dataResponse.readInt8() // Read length of List8
    case FormatCode.List32:
      dataResponse.forward(4)
      return dataResponse.readInt32()
    case FormatCode.Vbin8:
      return dataResponse.readUInt8()
    case FormatCode.Vbin32:
      return dataResponse.readUInt32()
    case FormatCode.Str8:
    case FormatCode.Sym8:
      if (skipByte) dataResponse.forward(1)
      return dataResponse.readString8()
    case FormatCode.Str32:
    case FormatCode.Sym32:
      if (skipByte) dataResponse.forward(1)
      return dataResponse.readString32()
    case FormatCode.Uint0:
      return 0
    case FormatCode.SmallUint:
      dataResponse.forward(1) // Skipping formatCode
      return dataResponse.readUInt8()
    case FormatCode.Uint:
      dataResponse.forward(1) // Skipping formatCode
      return dataResponse.readUInt32()
    case FormatCode.SmallInt:
      dataResponse.forward(1) // Skipping formatCode
      return dataResponse.readInt8()
    case FormatCode.Int:
      dataResponse.forward(1) // Skipping formatCode
      return dataResponse.readInt32()
    case FormatCode.Bool:
    case FormatCode.BoolTrue:
    case FormatCode.BoolFalse:
      return decodeBooleanType(dataResponse, true)
    case FormatCode.Null:
      dataResponse.forward(1) // Skipping formatCode
      return 0
    default:
      throw new Error(`ReadCompositeHeader Invalid type ${formatCode}`)
  }
}

export class BufferDataReader implements DataReader {
  private offset = 0

  constructor(private data: Buffer) {}

  readTo(size: number): DataReader {
    const ret = new BufferDataReader(this.data.subarray(this.offset, this.offset + size))
    this.offset += size
    return ret
  }

  readBufferOf(size: number): Buffer {
    const ret = Buffer.from(this.data.subarray(this.offset, this.offset + size))
    this.offset += size
    return ret
  }

  readToEnd(): DataReader {
    const ret = new BufferDataReader(this.data.subarray(this.offset))
    this.offset = this.data.length
    return ret
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

  readString8(): string {
    const sizeStr8 = this.readUInt8()
    const valueStr8 = this.data.toString("utf8", this.offset, this.offset + sizeStr8)
    this.offset += sizeStr8
    return valueStr8
  }

  readString32(): string {
    const sizeStr32 = this.readUInt32()
    const valueStr32 = this.data.toString("utf8", this.offset, this.offset + sizeStr32)
    this.offset += sizeStr32
    return valueStr32
  }

  rewind(count: number): void {
    this.offset -= count
  }

  forward(count: number): void {
    this.offset += count
  }

  position(): number {
    return this.offset
  }

  isAtEnd(): boolean {
    return this.offset === this.data.length
  }

  available(): number {
    return Buffer.byteLength(this.data) - this.offset
  }
}

function isTuneResponse(params: PossibleRawResponses): params is RawTuneResponse {
  return params.key === TuneResponse.key
}

function isConsumerUpdateQuery(params: PossibleRawResponses): params is RawConsumerUpdateQuery {
  return params.key === ConsumerUpdateQuery.key
}

function isHeartbeatResponse(params: PossibleRawResponses): params is RawHeartbeatResponse {
  return params.key === HeartbeatResponse.key
}

function isMetadataUpdateResponse(params: PossibleRawResponses): params is RawMetadataUpdateResponse {
  return params.key === MetadataUpdateResponse.key
}

function isDeliverResponseV1(params: PossibleRawResponses): params is RawDeliverResponse {
  return params.key === DeliverResponse.key && params.version === DeliverResponse.Version
}

function isDeliverResponseV2(params: PossibleRawResponses): params is RawDeliverResponseV2 {
  return params.key === DeliverResponseV2.key && params.version === DeliverResponseV2.Version
}

function isCreditResponse(params: PossibleRawResponses): params is RawCreditResponse {
  return params.key === CreditResponse.key
}

function isPublishConfirmResponse(params: PossibleRawResponses): params is RawPublishConfirmResponse {
  return params.key === PublishConfirmResponse.key
}

function isPublishErrorResponse(params: PossibleRawResponses): params is RawPublishErrorResponse {
  return params.key === PublishErrorResponse.key
}

export class ResponseDecoder {
  private responseFactories = new Map<number, AbstractTypeClass>()
  private emitter = new EventEmitter()
  private lastData = Buffer.from("")

  constructor(
    private listener: DecoderListenerFunc,
    private logger: Logger
  ) {
    this.addFactoryFor(PeerPropertiesResponse)
    this.addFactoryFor(SaslHandshakeResponse)
    this.addFactoryFor(SaslAuthenticateResponse)
    this.addFactoryFor(OpenResponse)
    this.addFactoryFor(CloseResponse)
    this.addFactoryFor(DeclarePublisherResponse)
    this.addFactoryFor(DeletePublisherResponse)
    this.addFactoryFor(CreateStreamResponse)
    this.addFactoryFor(CreateSuperStreamResponse)
    this.addFactoryFor(DeleteStreamResponse)
    this.addFactoryFor(DeleteSuperStreamResponse)
    this.addFactoryFor(QueryPublisherResponse)
    this.addFactoryFor(SubscribeResponse)
    this.addFactoryFor(UnsubscribeResponse)
    this.addFactoryFor(StreamStatsResponse)
    this.addFactoryFor(StoreOffsetResponse)
    this.addFactoryFor(QueryOffsetResponse)
    this.addFactoryFor(MetadataResponse)
    this.addFactoryFor(ExchangeCommandVersionsResponse)
    this.addFactoryFor(RouteResponse)
    this.addFactoryFor(PartitionsResponse)
  }

  add(data: Buffer, getCompressionBy: (type: CompressionType) => Compression) {
    const dataReader = new BufferDataReader(Buffer.concat([this.lastData, data]))
    this.lastData = Buffer.from("")

    while (!dataReader.isAtEnd()) {
      const { completed, response } = decode(dataReader, getCompressionBy, this.logger)

      if (!completed) {
        this.lastData = response
        continue
      }

      if (isHeartbeatResponse(response)) {
        this.logger.debug(`heartbeat received from the server: ${inspect(response)}`)
      } else if (isTuneResponse(response)) {
        this.emitTuneResponseReceived(response)
        this.logger.debug(`tune received from the server: ${inspect(response)}`)
      } else if (isConsumerUpdateQuery(response)) {
        this.emitter.emit("consumer_update_query", new ConsumerUpdateQuery(response))
        this.logger.debug(`consumer update query received from the server: ${inspect(response)}`)
      } else if (isMetadataUpdateResponse(response)) {
        this.emitter.emit("metadata_update", new MetadataUpdateResponse(response))
        this.logger.debug(`metadata update received from the server: ${inspect(response)}`)
      } else if (isDeliverResponseV1(response)) {
        this.emitter.emit("deliverV1", new DeliverResponse(response))
        this.logger.debug(`deliverV1 received from the server: ${inspect(response)}`)
      } else if (isDeliverResponseV2(response)) {
        this.emitter.emit("deliverV2", new DeliverResponseV2(response))
        this.logger.debug(`deliverV2 received from the server: ${inspect(response)}`)
      } else if (isCreditResponse(response)) {
        this.logger.debug(`credit received from the server: ${inspect(response)}`)
        this.emitter.emit("credit_response", new CreditResponse(response))
      } else if (isPublishConfirmResponse(response)) {
        this.emitter.emit("publish_confirm", new PublishConfirmResponse(response))
        this.logger.debug(`publish confirm received from the server: ${inspect(response)}`)
      } else if (isPublishErrorResponse(response)) {
        this.emitter.emit("publish_error", new PublishErrorResponse(response))
        this.logger.debug(`publish error received from the server: ${inspect(response)}`)
      } else {
        this.emitResponseReceived(response)
      }
    }
  }

  public on(event: "consumer_update_query", listener: ConsumerUpdateQueryListener): void
  public on(event: "metadata_update", listener: MetadataUpdateListener): void
  public on(event: "credit_response", listener: CreditListener): void
  public on(event: "publish_confirm", listener: PublishConfirmListener): void
  public on(event: "publish_error", listener: PublishErrorListener): void
  public on(event: "deliverV1", listener: DeliverListener): void
  public on(event: "deliverV2", listener: DeliverV2Listener): void
  public on(
    event:
      | "metadata_update"
      | "credit_response"
      | "publish_confirm"
      | "publish_error"
      | "deliverV1"
      | "deliverV2"
      | "consumer_update_query",
    listener:
      | MetadataUpdateListener
      | DeliverListener
      | DeliverV2Listener
      | CreditListener
      | PublishConfirmListener
      | PublishErrorListener
      | ConsumerUpdateQueryListener
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
      throw new Error(`Unknown response ${key}`)
    }
    return value
  }
}
