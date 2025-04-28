import { expect } from "chai"
import { isString } from "../../src/util"

describe("Util tests", () => {
  describe("isString", () => {
    it("return false with a number", () => {
      const value = 1

      expect(isString(value)).false
    })

    it("return false with a boolean", () => {
      const value = false

      expect(isString(value)).false
    })

    it("return true with a string", () => {
      const value = "abc"

      expect(isString(value)).true
    })

    it("return true with an empty string", () => {
      const value = ""

      expect(isString(value)).true
    })
  })
})
