"use strict"

import { ResponseCode } from "../util"

export type TResponseCode = (typeof ResponseCode)[keyof typeof ResponseCode]

/**
 * Provides distinct domain exception for the package. Contains the optional
 * RabbitMQ Stream protocol response code for more convenient processing.
 *
 * @param message A custom error message.
 * @param rmqStreamResponseCode The above mentioned response code.
 *
 * @see https://github.com/rabbitmq/rabbitmq-server/blob/main/deps/rabbitmq_stream/docs/PROTOCOL.adoc#response-codes
 *
 * @example Selectively manage the exception type and react differently.
 *
 * ```typescript
 * let result: any;
 *
 * const isRethrowable = (error_: Error) => {
 *     const isGenericError = error_ instanceof Code51Exception;
 *     const isNonManagedResponseCode = (error_ as Code51Exception).code !== ResponseCode.NoOffset;
 *
 *     return isGenericError && isNonManagedResponseCode;
 * };
 *
 * try {
 *     result = consumer.queryOffset();
 *     // ... process result
 * } catch (error_) {
 *     if (isRethrowable(error_)) { throw error_; }
 *
 *     const error = error_ as Code51Exception;
 *     if (error.code === ResponseCode.NoOffset) { return null; }
 *
 *     return result;
 * }
 * ```
 *
 */
export default class Code51Exception extends Error {
  readonly #code?: TResponseCode

  constructor(message: string, rmqStreamResponseCode?: TResponseCode) {
    super(message)

    Object.setPrototypeOf(this, new.target.prototype)

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
