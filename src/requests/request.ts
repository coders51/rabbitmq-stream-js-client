export interface Request {
  toBuffer(correlationId: number): Buffer
  readonly responseKey: number
  readonly key: number
  readonly version: 1
}
