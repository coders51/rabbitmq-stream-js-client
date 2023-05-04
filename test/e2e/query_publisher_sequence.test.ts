import { expect } from "chai"
import { randomUUID } from "crypto"
import { Connection } from "../../src"
import { createConnection, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"

describe("query publisher sequence", () => {
  const rabbit = new Rabbit()
  let streamName: string
  let connection: Connection

  beforeEach(async () => {
    connection = await createConnection()
    streamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    try {
      await connection.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("asking for the last sequence read from a publisher returns the last sequence id", async () => {
    const publisher = await createPublisher(streamName, connection)

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))
    await publisher.send(2n, Buffer.from(`test${randomUUID()}`))
    await publisher.send(3n, Buffer.from(`test${randomUUID()}`))
    await publisher.send(4n, Buffer.from(`test${randomUUID()}`))

    const lastPublishingId = await publisher.getLastPublishingId()

    expect(lastPublishingId).to.be.equal(4n)
  }).timeout(10000)

  it("asking for the last sequence read from a publisher whose never sent any message should return 0", async () => {
    const publisher = await createPublisher(streamName, connection)

    const lastPublishingId = await publisher.getLastPublishingId()

    expect(lastPublishingId).to.be.equal(0n)
    await connection.close()
  }).timeout(10000)
})
