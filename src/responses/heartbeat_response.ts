import { RawTuneResponse } from "./raw_response"
import { Response } from "./response"

export class HeartbeatResponse implements Response {
  static key = 0x0017
  static readonly Version = 1

  constructor(private response: RawTuneResponse) {
    if (this.response.key !== HeartbeatResponse.key) {
      throw new Error(`Unable to create ${HeartbeatResponse.name} from data of type ${this.response.key}`)
    }
  }

  get key() {
    return this.response.key
  }

  get correlationId(): number {
    return -1
  }

  get code(): number {
    return -1
  }

  get ok(): boolean {
    return true
  }
}
