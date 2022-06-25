import { RawResponse } from "./raw_response"
import { Response } from "./response"

interface AbstractTypeClass {
  name: string
  key: number
  new (...args: never[]): AbstractResponse
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
