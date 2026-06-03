import { ResponseCodeValue } from "./util"

export class StreamResponseError extends Error {

  constructor(message: string, private readonly streamResponseCode: ResponseCodeValue) {
    super(message)

    this.name = this.constructor.name
  }

  public get code(): ResponseCodeValue {
    return this.streamResponseCode
  }
}

export function isStreamResponseError(error: unknown): error is StreamResponseError {
  return error instanceof StreamResponseError
}
