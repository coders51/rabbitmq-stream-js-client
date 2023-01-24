export interface DataWriter {
  writeByte(Described: number): void
  writeUInt8(data: number): void
  writeUInt16(data: number): void
  writeUInt32(data: number): void
  writeUInt64(data: bigint): void
  writeInt32(length: number): void
  writeData(data: string | Buffer): void
  writeString(data: string): void
}
