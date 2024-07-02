import { SubscribeResponse } from "../responses/subscribe_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

const OFFSET_TYPE = {
  first: 1,
  last: 2,
  next: 3,
  numeric: 4,
  timestamp: 5,
} as const

export type OffsetType = keyof typeof OFFSET_TYPE

export class Offset {
  private constructor(
    public readonly type: OffsetType,
    public readonly value?: bigint
  ) {}

  write(writer: DataWriter) {
    writer.writeUInt16(OFFSET_TYPE[this.type])
    if (this.type === "numeric" && this.value !== null && this.value !== undefined) writer.writeUInt64(this.value)
    if (this.type === "timestamp" && this.value) writer.writeInt64(this.value)
  }

  static first() {
    return new Offset("first")
  }

  static last() {
    return new Offset("last")
  }

  static next() {
    return new Offset("next")
  }

  static offset(offset: bigint) {
    return new Offset("numeric", offset)
  }

  static timestamp(date: Date) {
    return new Offset("timestamp", BigInt(date.getTime()))
  }

  public clone() {
    return new Offset(this.type, this.value)
  }
}

export class SubscribeRequest extends AbstractRequest {
  static readonly Key = 0x0007
  static readonly Version = 1
  readonly key = SubscribeRequest.Key
  readonly responseKey = SubscribeResponse.key
  private readonly _properties: { key: string; value: string }[] = []

  constructor(
    private params: {
      subscriptionId: number
      stream: string
      credit: number
      offset: Offset
      properties?: Record<string, string>
    }
  ) {
    super()
    if (params.properties)
      this._properties = Object.keys(params.properties).map((key) => ({ key, value: params.properties![key] }))
  }

  protected writeContent(writer: DataWriter): void {
    writer.writeUInt8(this.params.subscriptionId)
    writer.writeString(this.params.stream)
    this.params.offset.write(writer)
    writer.writeUInt16(this.params.credit)
    writer.writeUInt32(this._properties.length)
    this._properties.forEach(({ key, value }) => {
      writer.writeString(key)
      writer.writeString(value)
    })
  }
}
