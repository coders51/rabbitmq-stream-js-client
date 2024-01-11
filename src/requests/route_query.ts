import { RouteResponse } from "../responses/route_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class RouteQuery extends AbstractRequest {
  readonly responseKey = RouteResponse.key
  static readonly Key = 0x0018
  static readonly Version = 1
  readonly key = RouteQuery.Key

  constructor(private params: { routingKey: string; superStream: string }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.params.routingKey)
    writer.writeString(this.params.superStream)
  }
}
