import { HeartbeatResponse } from "../responses/heartbeat_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class HeartbeatRequest extends AbstractRequest {
  readonly responseKey = HeartbeatResponse.key
  readonly key = 0x0017

  writeContent(_b: DataWriter) {
    return
  }

  toBuffer(): Buffer {
    return super.toBuffer(0)
  }
}
