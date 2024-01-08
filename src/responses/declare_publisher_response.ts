import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class DeclarePublisherResponse extends AbstractResponse {
  static key = 0x8001
  static readonly Version = 1

  readonly properties: Record<string, string> = {}

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(DeclarePublisherResponse)
  }
}
