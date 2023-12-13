import { expect } from "chai"
import { BufferDataWriter } from "../../src/requests/abstract_request"
describe("Buffer Data Writer functionalities", () => {
  const bufferMaxSize = 1024
  const bufferInitialSize = 1
  const stringPayload = "a long string that requires the buffer to grow"

  it("allocate a functioning buffer data writer", () => {
    const bufferSizeParams = { maxSize: bufferMaxSize }
    const b = new BufferDataWriter(Buffer.alloc(bufferInitialSize), 0, bufferSizeParams)
    b.writeByte(1)

    const result = b.toBuffer()

    expect(result).eql(Buffer.from([1]))
  })

  it("grow the buffer when needed", () => {
    const bufferSizeParams = { maxSize: bufferMaxSize }
    const b = new BufferDataWriter(Buffer.alloc(bufferInitialSize), 0, bufferSizeParams)

    b.writeString(stringPayload)

    const result = b.toBuffer()
    const header = result.slice(0, 2)
    const pl = result.slice(2)
    expect(header).eql(Buffer.from([0, 46]))
    expect(pl.length).eql(46)
    expect(pl.toString()).eql(stringPayload)
  })

  it("the buffer max size is a hard limit", () => {
    const maxSize = 32
    const bufferSizeParams = { maxSize: maxSize }
    const b = new BufferDataWriter(Buffer.alloc(bufferInitialSize), 0, bufferSizeParams)

    b.writeString(stringPayload)

    const result = b.toBuffer()
    const pl = result.slice(2)
    expect(pl.toString()).eql("a long string that requires th")
  })
})
