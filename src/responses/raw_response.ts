export interface DataReader {
  readTo(size: number): DataReader
  readToEnd(): DataReader

  readUInt16(): number
  readUInt32(): number
  readInt32(): number
  readUInt64(): bigint
  readString(): string
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

export interface RawHeartbeatResponse {
  key: 0x0014
  version: number
}
