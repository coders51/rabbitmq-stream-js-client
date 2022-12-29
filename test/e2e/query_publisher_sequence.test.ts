import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect } from "../../src"
import { Rabbit } from "../support/rabbit"

describe("query publisher sequence", () => {
  const rabbit = new Rabbit()
  const testStreamName = "test-stream"
  const publisherRef = randomUUID()

  beforeEach(async () => {
    await rabbit.createStream(testStreamName)
  })

  afterEach(async () => {
    await rabbit.deleteStream(testStreamName)
  })

  it("asking for the last sequence read from a publisher returns the last sequence id", async () => {
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
    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))
    await publisher.send(2n, Buffer.from(`test${randomUUID()}`))
    await publisher.send(3n, Buffer.from(`test${randomUUID()}`))
    await publisher.send(4n, Buffer.from(`test${randomUUID()}`))

    const lastPublishingId = await publisher.getLastPublishingId()

    expect(lastPublishingId).to.be.equal(4n)
    await connection.close()
  }).timeout(10000)

  it("asking for the last sequence read from a publisher whose never sent any message should return 0", async () => {
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

    const lastPublishingId = await publisher.getLastPublishingId()

    expect(lastPublishingId).to.be.equal(0n)
    await connection.close()
  }).timeout(10000)
})
