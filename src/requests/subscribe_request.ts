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

/**
 * The position in a stream from which a consumer starts reading
 *
 * Offsets are created through the static factory methods rather than the
 * constructor, e.g. `Offset.first()` or `Offset.offset(42n)`.
 */
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

  /** Start reading from the first available offset in the stream */
  static first() {
    return new Offset("first")
  }

  /** Start reading from the last chunk of messages currently in the stream */
  static last() {
    return new Offset("last")
  }

  /** Start reading from the next offset to be written (i.e. only new messages) */
  static next() {
    return new Offset("next")
  }

  /**
   * Start reading from a specific numeric offset
   *
   * @param offset - The offset to start from
   */
  static offset(offset: bigint) {
    return new Offset("numeric", offset)
  }

  /**
   * Start reading from the first message stored after the given timestamp
   *
   * @param date - The point in time to start reading from
   */
  static timestamp(date: Date) {
    return new Offset("timestamp", BigInt(date.getTime()))
  }

  /** Create an independent copy of this offset */
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
