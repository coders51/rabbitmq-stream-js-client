import { CreateStreamResponse } from "../responses/create_stream_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export interface CreateStreamArguments {
  "queue-leader-locator"?: "random" | "client-local" | "least-leaders"
  "max-age"?: string
  "stream-max-segment-size-bytes"?: number
  "initial-cluster-size"?: number
  "max-length-bytes"?: number
}

export class CreateStreamRequest extends AbstractRequest {
  readonly responseKey = CreateStreamResponse.key
  static readonly Key = 0x000d
  static readonly Version = 1
  readonly key = CreateStreamRequest.Key
  private readonly _arguments: { key: keyof CreateStreamArguments; value: string | number }[] = []
  private readonly stream: string

  constructor(params: { stream: string; arguments?: CreateStreamArguments }) {
    super()
    if (params.arguments) {
      this._arguments = (Object.keys(params.arguments) as Array<keyof CreateStreamArguments>).map((key) => {
        return {
          key,
          value: params.arguments![key] ?? "",
        }
      })
    }

    this.stream = params.stream
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.stream)
    writer.writeUInt32(this._arguments?.length ?? 0)
    this._arguments.forEach(({ key, value }) => {
      writer.writeString(key)
      writer.writeString(value.toString())
    })
  }
}
