export interface DataWriter {
  writeByte(Described: number): void
  writeUInt8(data: number): void
  writeUInt16(data: number): void
  writeUInt32(data: number): void
  writeUInt64(data: bigint): void
<<<<<<< HEAD
  writeInt32(length: number): void
  writeData(data: string | Buffer): void
  writeString(data: string): void
=======
>>>>>>> c56dfb3 (add subscribe command)
  writeInt64(data: bigint): void
}
