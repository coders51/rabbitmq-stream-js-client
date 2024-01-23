import { BufferDataWriter } from "./buffer_data_writer"
import { DataWriter } from "./data_writer"
import { BufferSizeParams, Request } from "./request"

export abstract class AbstractRequest implements Request {
  abstract get key(): number
  abstract get responseKey(): number
  abstract get version(): number

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
