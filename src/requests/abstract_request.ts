import { DEFAULT_FRAME_MAX } from "../util"
import { DataWriter } from "./data_writer"
import { BufferSizeParams, Request } from "./request"

export class BufferDataWriter implements DataWriter {
  private _offset = 0
  private readonly maxBufferSize: number
  private readonly growthTriggerRatio: number
  private readonly sizeMultiplier: number

  constructor(private buffer: Buffer, startFrom: number, bufferSizeParameters?: BufferSizeParams) {
    this._offset = startFrom
    this.maxBufferSize = bufferSizeParameters?.maxSize ?? 1048576
    this.growthTriggerRatio = bufferSizeParameters?.maxRatio ?? 0.9
    this.sizeMultiplier = bufferSizeParameters?.multiplier ?? 2
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
    if ((this._offset + additionalBytes) / this.buffer.length > this.growthTriggerRatio) {
      this.growBuffer(additionalBytes)
    }
  }

  private growBuffer(requiredBytes: number) {
    const newSize = this.getNewSize(requiredBytes)
    const data = Buffer.from(this.buffer)
    this.buffer = Buffer.alloc(newSize)
    data.copy(this.buffer, 0)
  }

  private getNewSize(requiredBytes: number) {
    const requiredNewSize = this.buffer.length * this.sizeMultiplier + this._offset + requiredBytes
    if (this.maxBufferSize === DEFAULT_FRAME_MAX) return requiredNewSize
    return Math.min(requiredNewSize, this.maxBufferSize)
  }
}
export abstract class AbstractRequest implements Request {
  abstract get key(): number
  abstract get responseKey(): number
  readonly version = 1

  toBuffer(bufferSizeParams?: BufferSizeParams, correlationId?: number): Buffer {
    const initialSize = bufferSizeParams?.initialSize ?? 65536
    const dataWriter = new BufferDataWriter(Buffer.alloc(initialSize), 4, bufferSizeParams)
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
