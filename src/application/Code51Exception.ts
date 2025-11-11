"use strict"

import { ResponseCode } from "../util"

type TResponseCode = (typeof ResponseCode)[keyof typeof ResponseCode]

export default class Code51Exception extends Error {
  readonly #code?: TResponseCode

  constructor(message: string, rmqStreamResponseCode?: TResponseCode) {
    super(message)

    this.name = this.constructor.name
    this.#code = rmqStreamResponseCode ?? undefined

    // Maintains proper stack trace for where our error was thrown (only available on V8)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor)
    }
  }

  public get code(): TResponseCode | undefined {
    return this.#code
  }
}
