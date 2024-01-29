import { HeartbeatResponse } from "../responses/heartbeat_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class HeartbeatRequest extends AbstractRequest {
  readonly responseKey = HeartbeatResponse.key
  static readonly Key = 0x0017
  static readonly Version = 1
  readonly key = HeartbeatRequest.Key

  writeContent(_b: DataWriter) {
    return
  }
}
