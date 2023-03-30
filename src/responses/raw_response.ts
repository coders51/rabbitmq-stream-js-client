export interface DataReader {
  readToBuffer(length: number): Buffer
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

  rewind(count: number): void
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

export interface RawDeliverResponse {
  size: number
  key: 0x0008
  version: number
  subscriptionId: number
  messages: Buffer[]
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

export interface RawHeartbeatResponse {
  key: 0x0014
  version: number
}
