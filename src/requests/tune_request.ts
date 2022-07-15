import { TuneResponse } from "../responses/tune_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class TuneRequest extends AbstractRequest {
  readonly responseKey = TuneResponse.key
  readonly key = 0x0014

  constructor(private params: { frameMax: number; heartbeat: number }) {
    super()
  }

  writeContent(b: DataWriter) {
    b.writeUInt32(this.params.frameMax)
    b.writeUInt32(this.params.heartbeat)
  }
}
