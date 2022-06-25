import { Request } from "./request"

export abstract class AbstractRequest implements Request {
  abstract get key(): number
  abstract get responseKey(): number
  readonly version = 1

  toBuffer(correlationId: number): Buffer {
    let offset = 4
    const b = Buffer.alloc(1024)
    offset = b.writeUInt16BE(this.key, offset)
    offset = b.writeUInt16BE(this.version, offset)
    offset = b.writeUInt32BE(correlationId, offset)

    offset = this.writeContent(b, offset)

    b.writeUInt32BE(offset - 4, 0)
    return b.slice(0, offset)
  }
  protected abstract writeContent(b: Buffer, offset: number): number
}

export function writeString(buffer: Buffer, offset: number, s: string) {
  const newOffset = buffer.writeInt16BE(s.length, offset)
  const written = buffer.write(s, newOffset, "utf8")
  return newOffset + written
}
