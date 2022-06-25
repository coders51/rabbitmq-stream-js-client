export interface Response {
  code: number
  ok: boolean
  key: number
  correlationId: number
}

export function readString(payload: Buffer, offset: number): { offset: number; value: string } {
  const size = payload.readUInt16BE(offset)
  const value = payload.toString("utf8", offset + 2, offset + 2 + size)
  return { offset: offset + 2 + size, value }
}
