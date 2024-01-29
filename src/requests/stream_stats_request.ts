import { StreamStatsResponse } from "../responses/stream_stats_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class StreamStatsRequest extends AbstractRequest {
  readonly responseKey = StreamStatsResponse.key
  static readonly Key = 0x001c
  static readonly Version = 1
  readonly key = StreamStatsRequest.Key

  constructor(private streamName: string) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.streamName)
  }
}
