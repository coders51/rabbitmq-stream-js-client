import { TuneResponse } from "../responses/tune_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class TuneRequest extends AbstractRequest {
  readonly responseKey = TuneResponse.key
  static readonly Key = 0x0014
  static readonly Version = 1
  readonly key = TuneRequest.Key

  constructor(private params: { frameMax: number; heartbeat: number }) {
    super()
  }

  writeContent(b: DataWriter) {
    b.writeUInt32(this.params.frameMax)
    b.writeUInt32(this.params.heartbeat)
  }
}
