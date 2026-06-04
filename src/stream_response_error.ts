export class StreamResponseError extends Error {
  constructor(
    message: string,
    private readonly streamResponseCode: number
  ) {
    super(message)

    this.name = this.constructor.name
  }

  public get code(): number {
    return this.streamResponseCode
  }
}

export function isStreamResponseError(error: unknown): error is StreamResponseError {
  return error instanceof StreamResponseError
}
