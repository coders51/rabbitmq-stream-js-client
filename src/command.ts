export interface Command {
  toBuffer(correlationId: number): Buffer
  readonly responseKey: number
  readonly key: number
  readonly version: 1
}
