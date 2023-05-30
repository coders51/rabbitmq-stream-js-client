import { StreamStatsResponse } from "../responses/stream_stats_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class StreamStatsRequest extends AbstractRequest {
  readonly responseKey = StreamStatsResponse.key
  readonly key = 0x001c

  constructor(private streamName: string) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.streamName)
  }
}
