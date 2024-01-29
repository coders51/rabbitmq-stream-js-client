import { Message } from "../publisher"

export interface DataReader {
  readBufferOf(length: number): Buffer
  readTo(size: number): DataReader
  readToEnd(): DataReader
  readInt8(): number
  readUInt8(): number
  readUInt16(): number
  readUInt32(): number
  readInt32(): number
  readUInt64(): bigint
  readInt64(): bigint
  readString(): string
  readString8(): string
  readString32(): string
  rewind(count: number): void
  forward(count: number): void
  position(): number
  isAtEnd(): boolean
  available(): number
}

export interface RawResponse {
  size: number
  key: number
  version: number
  correlationId: number
  code: number
  payload: DataReader
}

export interface RawTuneResponse {
  size: number
  key: 0x0014
  version: number
  frameMax: number
  heartbeat: number
}

export interface RawConsumerUpdateQueryResponse {
  size: number
  key: 0x001a
  version: number
  correlationId: number
  subscriptionId: number
  active: number
}

export interface RawDeliverResponse {
  size: number
  key: 0x0008
  version: number
  subscriptionId: number
  messages: Message[]
}

export interface RawDeliverResponseV2 {
  size: number
  key: 0x0008
  version: number
  subscriptionId: number
  committedChunkId: bigint
  messages: Message[]
}

export interface RawMetadataUpdateResponse {
  size: number
  key: 0x0010
  version: number
  metadataInfo: MetadataInfo
}

export interface MetadataInfo {
  code: number
  stream: string
}

export interface RawCreditResponse {
  size: number
  key: 0x8009
  version: number
  responseCode: number
  subscriptionId: number
}

export interface RawHeartbeatResponse {
  key: 0x0014
  version: number
}

export interface RawPublishConfirmResponse {
  size: number
  key: 0x0003
  version: number
  publisherId: number
  publishingIds: bigint[]
}

export interface RawPublishErrorResponse {
  size: number
  key: 0x0004
  version: number
  publisherId: number
  publishingId: bigint
  code: number
}
