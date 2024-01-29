export type BufferSizeSettings = {
  initialSize?: number
  maxRatio?: number
  multiplier?: number
}

export type BufferSizeParams = BufferSizeSettings & { maxSize: number }

export interface Request {
  toBuffer(bufferSizeParams?: BufferSizeParams, correlationId?: number): Buffer
  readonly responseKey: number
  readonly key: number
  readonly version: number
}
