export interface DataReader {
  slice(): DataReader

  readUInt16(): number
  readUInt32(): number
  readInt32(): number
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
