import { expect } from "chai"

import Code51Exception from "../../../src/application/Code51Exception"
import { ResponseCode } from "../../../src/util"

describe("[unit] Code51Exception Test", () => {
  it("+constructor() #1: Should create Code51Exception expected default object", () => {
    const expected = "A message"
    const actual = new Code51Exception(expected)

    expect(actual).instanceOf(Code51Exception)
    expect(actual.message).eql(expected)
    expect(actual.code).eql(undefined)
  })

  it("+constructor() #2: Should create Code51Exception expected object with expectd response code", () => {
    const expected = ResponseCode.NoOffset
    const actual = new Code51Exception("a message", expected)

    expect(actual.code).eql(expected)
  })

  it("Should throw expected Code51Exception exception", () => {
    const expected = { message: "A message", code: ResponseCode.SubscriptionIdDoesNotExist }

    try {
      throw new Code51Exception(expected.message, expected.code)
    } catch (error_) {
      const error = error_ as Code51Exception

      expect(error).instanceOf(Code51Exception)
      expect(error.message).eql(expected.message)
      expect(error.code).eql(expected.code)
    }
  })
})

// Assert: proper stack trace
