export interface DataWriter {
  writeData(data: string): void
  writeUInt8(data: number): void
  writeUInt32(data: number): void
  writeString(data: string): void
}
