export interface DataWriter {
  writeByte(Described: number): unknown
  writeData(data: string | Buffer): void
  writeUInt8(data: number): void
  writeUInt16(data: number): void
  writeUInt32(data: number): void
  writeString(data: string): void
  writeUInt64(data: bigint): void
}
