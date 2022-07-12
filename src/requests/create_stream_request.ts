import { CreateStreamResponse } from "../responses/create_stream_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export const validArguments = [
  "x-queue-leader-locator",
  "x-max-age",
  "x-stream-max-segment-size-bytes",
  "x-initial-cluster-size",
  "x-max-length-bytes",
]

export class CreateStreamRequest extends AbstractRequest {
  readonly responseKey = CreateStreamResponse.key
  readonly key = 0x000d
  private readonly _arguments: { key: string; value: string }[] = []
  private readonly stream: string

  constructor(params: { stream: string; arguments: Record<string, string> }) {
    super()
    this._arguments = Object.keys(params.arguments)
      .filter((key) => validArguments.includes(key))
      .map((key) => ({ key, value: params.arguments[key] }))
    this.stream = params.stream
  }

  writeContent(b: DataWriter) {
    b.writeString(this.stream)
    b.writeUInt32(this._arguments.length)
    this._arguments.forEach(({ key, value }) => {
      b.writeString(key)
      b.writeString(value)
    })
  }
}
