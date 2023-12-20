import { expect } from "chai"
import { randomUUID } from "crypto"
import { Client } from "../../src"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { username, password } from "../support/util"

describe("query publisher sequence", () => {
  let streamName: string
  let client: Client
  let publisherRef: string
  const rabbit = new Rabbit(username, password)

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    publisherRef = randomUUID()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("asking for the last sequence read from a publisher returns the last sequence id", async () => {
    const publisher = await client.declarePublisher({ stream: streamName, publisherRef })
    await publisher.basicSend(1n, Buffer.from(`test${randomUUID()}`))
    await publisher.basicSend(2n, Buffer.from(`test${randomUUID()}`))
    await publisher.basicSend(3n, Buffer.from(`test${randomUUID()}`))
    await publisher.basicSend(4n, Buffer.from(`test${randomUUID()}`))
    await publisher.flush()

    const lastPublishingId = await publisher.getLastPublishingId()

    expect(lastPublishingId).to.be.equal(4n)
  }).timeout(10000)

  it("asking for the last sequence read from a publisher whose never sent any message should return 0", async () => {
    const publisher = await client.declarePublisher({ stream: streamName, publisherRef })

    const lastPublishingId = await publisher.getLastPublishingId()

    expect(lastPublishingId).to.be.equal(0n)
    await client.close()
  }).timeout(10000)
})
