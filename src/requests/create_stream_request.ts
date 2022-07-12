import { CreateStreamResponse } from "../responses/create_stream_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export interface CreateStreamArguments {
  "x-queue-leader-locator"?: string
  "x-max-age"?: string
  "x-stream-max-segment-size-bytes"?: number
  "x-initial-cluster-size"?: number
  "x-max-length-bytes"?: number
}

export class CreateStreamRequest extends AbstractRequest {
  readonly responseKey = CreateStreamResponse.key
  readonly key = 0x000d

  private readonly _arguments: { key: keyof CreateStreamArguments; value: string | number }[] = []
  private readonly stream: string

  constructor(params: { stream: string; arguments: CreateStreamArguments }) {
    super()
    this._arguments = (Object.keys(params.arguments) as Array<keyof CreateStreamArguments>).map((key) => {
      return {
        key,
        value: params.arguments[key] ?? "",
      }
    })

    this.stream = params.stream
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.stream)
    writer.writeUInt32(this._arguments.length)
    this._arguments.forEach(({ key, value }) => {
      writer.writeString(key)
      writer.writeString(value.toString())
    })
  }
}
