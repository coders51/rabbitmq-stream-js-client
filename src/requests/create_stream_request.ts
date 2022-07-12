import { CreateStreamResponse } from "../responses/create_stream_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export interface validArguments {
  "x-queue-leader-locator"?: string
  "x-max-age"?: string
  "x-stream-max-segment-size-bytes"?: string
  "x-initial-cluster-size"?: string
  "x-max-length-bytes"?: string
}

export class CreateStreamRequest extends AbstractRequest {
  readonly responseKey = CreateStreamResponse.key
  readonly key = 0x000d
  private readonly _arguments: { key: keyof validArguments; value: string | undefined }[] = []
  private readonly stream: string

  constructor(params: { stream: string; arguments: validArguments }) {
    super()
    this._arguments = (Object.keys(params.arguments) as Array<keyof validArguments>).map((key) => {
      return {
        key,
        value: params.arguments[key],
      }
    })
    this.stream = params.stream
  }

  writeContent(b: DataWriter) {
    b.writeString(this.stream)
    b.writeUInt32(this._arguments.length)
    this._arguments.forEach(({ key, value }) => {
      if (value) {
        b.writeString(key)
        b.writeString(value)
      }
    })
  }
}
