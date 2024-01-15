import { expect } from "chai"
import { Client, Consumer } from "../../src"
import { Offset } from "../../src/requests/subscribe_request"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync, getTestNodesFromEnv, password, username } from "../support/util"
import { createClient, createConsumer } from "../support/fake_data"

describe("close consumer", () => {
  const rabbit = new Rabbit(username, password)
  const testStreamName = "test-stream"
  let client: Client

  beforeEach(async () => {
    await rabbit.createStream(testStreamName)
    client = await createClient(username, password)
  })

  afterEach(async () => {
    await client.close()
    await rabbit.deleteStream(testStreamName)
  })

  it("closing a consumer in an existing stream", async () => {
    await client.declarePublisher({ stream: testStreamName })
    const consumer = await client.declareConsumer({ stream: testStreamName, offset: Offset.first() }, console.log)

    const response = await client.closeConsumer(consumer.consumerId)

    expect(response).eql(true)
    expect(client.consumerCounts()).eql(0)
    await eventually(() => {
      const { readable } = consumer.getConnectionInfo()
      expect(readable).eql(false)
    })
  }).timeout(5000)

  it("closing a non-existing consumer should rise an error", async () => {
    const nonExistingConsumerId = 123456
    await client.declarePublisher({ stream: testStreamName })

    await expectToThrowAsync(() => client.closeConsumer(nonExistingConsumerId), Error)
  }).timeout(5000)

  it("closing a consumer does not close the underlying connection if it is still in use", async () => {
    const consumersToCreate = getTestNodesFromEnv().length + 1
    const consumers = new Map<number, Consumer[]>()
    for (let i = 0; i < consumersToCreate; i++) {
      const consumer = await createConsumer(testStreamName, client)
      const { localPort } = consumer.getConnectionInfo()
      const key = localPort || -1
      const currentConsumers = consumers.get(key) || []
      currentConsumers.push(consumer)
      consumers.set(key, currentConsumers)
    }
    const sharingConsumers = Array.from(consumers.values()).find((consumerArrays) => consumerArrays.length >= 2) || []

    await client.closeConsumer(sharingConsumers[0].consumerId)

    const consumer2Info = sharingConsumers[1].getConnectionInfo()
    expect(sharingConsumers.length).gte(2)
    expect(consumer2Info.readable).eql(true)
  }).timeout(5000)

  it("after closing all consumers the underlying connections are closed as well", async () => {
    const consumersToCreate = getTestNodesFromEnv().length + 1
    const consumers = new Map<number, Consumer[]>()
    for (let i = 0; i < consumersToCreate; i++) {
      const consumer = await createConsumer(testStreamName, client)
      const { localPort } = consumer.getConnectionInfo()
      const key = localPort || -1
      const currentConsumers = consumers.get(key) || []
      currentConsumers.push(consumer)
      consumers.set(key, currentConsumers)
    }
    const sharingConsumers = Array.from(consumers.values()).find((consumerArrays) => consumerArrays.length >= 2) || []

    sharingConsumers.forEach((c) => client.closeConsumer(c.consumerId))

    await eventually(() => {
      expect(sharingConsumers).satisfies((consumerArrays: Consumer[]) =>
        consumerArrays.every((consumer) => {
          const { readable } = consumer.getConnectionInfo()
          return readable !== true
        })
      )
    })
  }).timeout(5000)
})
