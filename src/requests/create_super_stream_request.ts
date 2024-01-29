import { CreateSuperStreamResponse } from "../responses/create_super_stream_response"
import { AbstractRequest } from "./abstract_request"
import { CreateStreamArguments } from "./create_stream_request"
import { DataWriter } from "./data_writer"

export interface CreateSuperStreamParams {
  streamName: string
  partitions: string[]
  bindingKeys: string[]
  arguments?: CreateStreamArguments
}

export class CreateSuperStreamRequest extends AbstractRequest {
  readonly responseKey = CreateSuperStreamResponse.key
  static readonly Key = 0x001d
  static readonly Version = 1
  readonly key = CreateSuperStreamRequest.Key
  private readonly _arguments: { key: keyof CreateStreamArguments; value: string | number }[] = []
  private readonly streamName: string
  private readonly partitions: string[]
  private readonly bindingKeys: string[]

  constructor(params: CreateSuperStreamParams) {
    super()
    if (params.arguments) {
      this._arguments = (Object.keys(params.arguments) as Array<keyof CreateStreamArguments>).map((key) => {
        return {
          key,
          value: params.arguments![key] ?? "",
        }
      })
    }
    this.streamName = params.streamName
    this.partitions = params.partitions
    this.bindingKeys = params.bindingKeys
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.streamName)
    writer.writeInt32(this.partitions.length)
    this.partitions.forEach((partition) => writer.writeString(partition))
    writer.writeInt32(this.bindingKeys.length)
    this.bindingKeys.forEach((bindingKey) => writer.writeString(bindingKey))
    writer.writeUInt32(this._arguments?.length ?? 0)
    this._arguments.forEach(({ key, value }) => {
      writer.writeString(key)
      writer.writeString(value.toString())
    })
  }
}
