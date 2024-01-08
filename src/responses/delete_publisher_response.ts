import { AbstractResponse } from "./abstract_response"
import { RawResponse } from "./raw_response"

export class DeletePublisherResponse extends AbstractResponse {
  static key = 0x8006
  static readonly Version = 1

  readonly properties: Record<string, string> = {}

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(DeletePublisherResponse)
  }
}
