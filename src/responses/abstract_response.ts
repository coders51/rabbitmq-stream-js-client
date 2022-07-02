import { RawResponse } from "./raw_response"
import { Response } from "./response"

export interface AbstractTypeClass {
  name: string
  key: number
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  new (...args: any[]): AbstractResponse
}

export abstract class AbstractResponse implements Response {
  constructor(protected response: RawResponse) {}

  protected verifyKey(type: AbstractTypeClass) {
    if (this.response.key !== type.key) {
      throw new Error(`Unable to create ${type.name} from data of type ${this.response.key}`)
    }
  }

  get key() {
    return this.response.key
  }

  get correlationId(): number {
    return this.response.correlationId
  }

  get code(): number {
    return this.response.code
  }

  get ok(): boolean {
    return this.code === 0x01
  }
}
