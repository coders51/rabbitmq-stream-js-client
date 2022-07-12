export interface DataWriter {
  writeData(data: string | Buffer): void
  writeUInt8(data: number): void
  writeUInt32(data: number): void
  writeString(data: string): void
  writeUInt64(publishingId: bigint): void
}
