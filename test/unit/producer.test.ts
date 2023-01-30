import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect } from "../../src"
import { Rabbit } from "../support/rabbit"

describe("Producer", () => {
  const rabbit = new Rabbit()
  const testStreamName = "test-stream"
  const publisherRef = randomUUID()

  beforeEach(async () => {
    await rabbit.createStream(testStreamName)
  })

  afterEach(async () => {
    await rabbit.deleteStream(testStreamName)
  })

  describe("Send: ", () => {
    it("boot is true", async () => {
      const connection = await connect({
        hostname: "localhost",
        port: 5552,
        username: "rabbit",
        password: "rabbit",
        vhost: "/",
        frameMax: 0,
        heartbeat: 0,
      })
      const publisher = await connection.declarePublisher({ stream: testStreamName, publisherRef, boot: true })

      await publisher.send(Buffer.from(`test${randomUUID()}`))

      const lastPublishingId = await publisher.getLastPublishingId()
      expect(lastPublishingId).to.be.equal(1n)
      await connection.close()
    }).timeout(10000)

    it("boot is false", async () => {
      const connection = await connect({
        hostname: "localhost",
        port: 5552,
        username: "rabbit",
        password: "rabbit",
        vhost: "/",
        frameMax: 0,
        heartbeat: 0,
      })
      const publisher = await connection.declarePublisher({ stream: testStreamName, publisherRef })

      await publisher.send(Buffer.from(`test${randomUUID()}`))

      const lastPublishingId = await publisher.getLastPublishingId()
      expect(lastPublishingId).to.be.equal(1n)
      await connection.close()
    }).timeout(10000)
  })
})
