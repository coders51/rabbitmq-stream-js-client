export interface DataWriter {
  writeByte(Described: number): void
  writeInt8(data: number): void
  writeInt16(data: number): void
  writeUInt8(data: number): void
  writeUInt16(data: number): void
  writeUInt32(data: number): void
  writeUInt64(data: bigint): void
  writeInt32(length: number): void
  writeData(data: string | Buffer): void
  writeString(data: string): void
  writeInt64(data: bigint): void
}
