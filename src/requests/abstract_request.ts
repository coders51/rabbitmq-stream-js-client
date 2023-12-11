import { DataWriter } from "./data_writer"
import { Request } from "./request"

export class BufferDataWriter implements DataWriter {
  private _offset = 0

  constructor(private readonly writeBufferMaxSize: number, private buffer: Buffer, startFrom: number) {
    this._offset = startFrom
  }

  get offset() {
    return this._offset
  }

  writePrefixSize() {
    this.buffer.writeUInt32BE(this._offset - 4, 0)
  }

  writeData(data: string | Buffer): void {
    this.growIfNeeded(Buffer.byteLength(data, "utf-8"))
    if (Buffer.isBuffer(data)) {
      this._offset += data.copy(this.buffer, this._offset)
      return
    }
    this._offset += this.buffer.write(data, this._offset)
  }

  writeByte(data: number): void {
    const bytes = 1
    this.growIfNeeded(bytes)
    this._offset = this.buffer.writeUInt8(data, this._offset)
  }

  writeInt8(data: number) {
    const bytes = 1
    this.growIfNeeded(bytes)
    this._offset = this.buffer.writeInt8(data, this._offset)
  }

  writeUInt8(data: number): void {
    const bytes = 1
    this.growIfNeeded(bytes)
    this._offset = this.buffer.writeUInt8(data, this._offset)
  }

  writeUInt16(data: number) {
    const bytes = 2
    this.growIfNeeded(bytes)
    this._offset = this.buffer.writeUInt16BE(data, this._offset)
  }

  writeUInt32(data: number): void {
    const bytes = 4
    this.growIfNeeded(bytes)
    this._offset = this.buffer.writeUInt32BE(data, this._offset)
  }

  writeInt32(data: number): void {
    const bytes = 4
    this.growIfNeeded(bytes)
    this._offset = this.buffer.writeInt32BE(data, this._offset)
  }

  writeUInt64(data: bigint): void {
    const bytes = 8
    this.growIfNeeded(bytes)
    this._offset = this.buffer.writeBigUInt64BE(data, this._offset)
  }

  writeInt64(data: bigint): void {
    const bytes = 8
    this.growIfNeeded(bytes)
    this._offset = this.buffer.writeBigInt64BE(data, this._offset)
  }

  writeString(data: string): void {
    const bytes = 2
    this.growIfNeeded(bytes)
    this._offset = this.buffer.writeInt16BE(data.length, this._offset)
    this.writeData(data)
  }

  toBuffer(): Buffer {
    return this.buffer.slice(0, this._offset)
  }

  private growIfNeeded(additionalBytes: number) {
    const maxRatio = 0.9
    if ((this._offset + additionalBytes) / this.buffer.length > maxRatio) {
      this.growBuffer(additionalBytes)
    }
  }

  private growBuffer(requiredBytes: number) {
    const newSize = Math.min(this.buffer.length * 2 + requiredBytes, this.writeBufferMaxSize)
    const data = Buffer.from(this.buffer)
    this.buffer = Buffer.alloc(newSize)
    data.copy(this.buffer, 0)
  }
}
export abstract class AbstractRequest implements Request {
  abstract get key(): number
  abstract get responseKey(): number
  readonly version = 1

  constructor(private readonly writeBufferMaxSize: number = 1048576) {}

  toBuffer(correlationId?: number): Buffer {
    const initialBufferSize = 65536
    const dataWriter = new BufferDataWriter(this.writeBufferMaxSize, Buffer.alloc(initialBufferSize), 4)
    dataWriter.writeUInt16(this.key)
    dataWriter.writeUInt16(this.version)
    if (typeof correlationId === "number") {
      dataWriter.writeUInt32(correlationId)
    }

    this.writeContent(dataWriter)

    dataWriter.writePrefixSize()
    return dataWriter.toBuffer()
  }

  protected abstract writeContent(writer: DataWriter): void
}
