import { expect } from "chai"

import { ResponseCode } from "../../src/util"
import { isStreamResponseError, StreamResponseError } from "../../src/stream_response_error"

describe("StreamResponseError Test", () => {
  it("isStreamResponseError should return true for StreamResponseError instances", () => {
    const error = new StreamResponseError("Test error", ResponseCode.StreamDoesNotExist)
    expect(isStreamResponseError(error)).to.be.true
  })

  it("isStreamResponseError should return false for non-StreamResponseError instances", () => {
    const nonError = { message: "Not an error", code: 999 }
    expect(isStreamResponseError(nonError)).to.be.false

    const genericError = new Error("Generic error")
    expect(isStreamResponseError(genericError)).to.be.false
  })
})
