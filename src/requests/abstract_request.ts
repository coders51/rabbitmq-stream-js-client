import { DataWriter } from "./data_writer"
import { Request } from "./request"

export class BufferDataWriter implements DataWriter {
  private _offset = 0

  constructor(private buffer: Buffer, startFrom: number) {
    this._offset = startFrom
  }

  get offset() {
    return this._offset
  }

  writePrefixSize() {
    this.buffer.writeUInt32BE(this._offset - 4, 0)
  }

  writeData(data: string): void {
    const written = this.buffer.write(data, this._offset)
    this._offset += written
  }

  writeUInt8(data: number): void {
    this._offset = this.buffer.writeUInt8(data, this._offset)
  }

  writeUInt16(data: number) {
    this._offset = this.buffer.writeUInt16BE(data, this._offset)
  }

  writeUInt32(data: number): void {
    this._offset = this.buffer.writeUInt32BE(data, this._offset)
  }

  writeInt32(data: number): void {
    this._offset = this.buffer.writeUInt32BE(data, this._offset)
  }

  writeString(data: string): void {
    this._offset = this.buffer.writeInt16BE(data.length, this._offset)
    this.writeData(data)
  }

  toBuffer(): Buffer {
    return this.buffer.slice(0, this._offset)
  }
}
export abstract class AbstractRequest implements Request {
  abstract get key(): number
  abstract get responseKey(): number
  readonly version = 1

  toBuffer(correlationId: number): Buffer {
    const dataWriter = new BufferDataWriter(Buffer.alloc(1024), 4)
    dataWriter.writeUInt16(this.key)
    dataWriter.writeUInt16(this.version)
    dataWriter.writeUInt32(correlationId)

    this.writeContent(dataWriter)

    dataWriter.writePrefixSize()
    return dataWriter.toBuffer()
  }

  protected abstract writeContent(writer: DataWriter): void
}
