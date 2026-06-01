export class StreamResponseError extends Error {
  readonly #code?: number

  constructor(message: string, rmqStreamResponseCode?: number) {
    super(message)

    this.name = this.constructor.name
    this.#code = rmqStreamResponseCode ?? undefined
  }

  public get code(): number | undefined {
    return this.#code
  }
}

export function isStreamResponseError(error: unknown): error is StreamResponseError {
  return error instanceof StreamResponseError
}
