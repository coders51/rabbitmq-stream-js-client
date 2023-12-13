import { expect } from "chai"
import { randomUUID } from "crypto"
import { PublishRequest } from "../../src/requests/publish_request"

describe("PublishRequest", () => {
  it("Produce a buffer for a long list of messages", () => {
    const publisherId = 1
    const maxFrameSize = 1024
    const messages = [...Array(100).keys()].map((idx) => {
      return { publishingId: BigInt(idx), message: { content: Buffer.from(randomUUID()) } }
    })
    const pr = new PublishRequest({ publisherId, messages })

    const written = pr.toBuffer({ maxSize: maxFrameSize })

    expect(written.byteLength).eql(5313)
  })
})
