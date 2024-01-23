import { OpenResponse } from "../responses/open_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export class OpenRequest extends AbstractRequest {
  static readonly Key = 0x0015
  static readonly Version = 1

  constructor(private params: { virtualHost: string }) {
    super()
  }

  writeContent(writer: DataWriter) {
    writer.writeString(this.params.virtualHost)
  }

  get key(): number {
    return OpenRequest.Key
  }
  get responseKey(): number {
    return OpenResponse.key
  }
  get version(): number {
    return OpenRequest.Version
  }
}
