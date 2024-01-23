import { StreamStatsResponse } from "../responses/stream_stats_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class StreamStatsRequest extends AbstractRequest {
  static readonly Key = 0x001c
  static readonly Version = 1

  constructor(private streamName: string) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.streamName)
  }

  get key(): number {
    return StreamStatsRequest.Key
  }
  get responseKey(): number {
    return StreamStatsResponse.key
  }
  get version(): number {
    return StreamStatsRequest.Version
  }
}
