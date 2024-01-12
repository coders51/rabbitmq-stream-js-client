import { expect } from "chai"
import { Client } from "../../src"
import { createClient, createConsumer, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { password, username } from "../support/util"

describe("connection management for clusters (applicable even on a single node)", () => {
  const rabbit = new Rabbit(username, password)
  let client: Client
  let streamName: string

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
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

  it("when we create a consumer, a new connection is opened", async () => {
    const clientConnectionInfo = client.getConnectionInfo()

    const consumer = await createConsumer(streamName, client)

    const consumerConnectionInfo = consumer.getConnectionInfo()
    expect(clientConnectionInfo.id).to.not.be.equal(consumerConnectionInfo.id)
  }).timeout(10000)

  it("when we create a publisher, a new connection is opened", async () => {
    const clientConnectionInfo = client.getConnectionInfo()

    const publisher = await createPublisher(streamName, client)

    const publisherConnectionInfo = publisher.getConnectionInfo()
    expect(clientConnectionInfo.id).to.not.be.equal(publisherConnectionInfo.id)
  }).timeout(10000)

  it("when we create a publisher, the connection should be done on the leader", async () => {
    const streamInfo = await rabbit.getQueue("%2F", streamName)
    const leader = streamInfo.node
    const [leaderHostName] = leader.split("@").slice(-1)

    const publisher = await createPublisher(streamName, client)

    const connectionInfo = publisher.getConnectionInfo()
    expect(connectionInfo.host).to.be.equal(leaderHostName)
  }).timeout(10000)

  it("closing the client closes all publisher and consumers - no connection is left hanging", async () => {
    await createConsumer(streamName, client)
    await createConsumer(streamName, client)
    await createPublisher(streamName, client)

    await client.close()

    expect(client.consumerCounts()).to.be.equal(0)
    expect(client.publisherCounts()).to.be.equal(0)
  }).timeout(10000)
})
