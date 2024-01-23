import { HeartbeatResponse } from "../responses/heartbeat_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class HeartbeatRequest extends AbstractRequest {
  static readonly Key = 0x0017
  static readonly Version = 1

  writeContent(_b: DataWriter) {
    return
  }

  get key(): number {
    return HeartbeatRequest.Key
  }
  get responseKey(): number {
    return HeartbeatResponse.key
  }
  get version(): number {
    return HeartbeatRequest.Version
  }
}
