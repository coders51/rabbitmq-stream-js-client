import { expect } from "chai"
import { randomUUID } from "node:crypto"
import { Client, Consumer } from "../../src"
import { computeExtendedConsumerId } from "../../src/consumer"
import { Offset } from "../../src/requests/subscribe_request"
import { getMaxSharedConnectionInstances } from "../../src/util"
import { createClient, createConsumer } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync, getTestNodesFromEnv, password, username } from "../support/util"

describe("close consumer", () => {
  const rabbit = new Rabbit(username, password)
  const testStreamName = "test-stream"
  let client: Client
  const previousMaxSharedClientInstances = process.env.MAX_SHARED_CLIENT_INSTANCES

  before(() => {
    process.env.MAX_SHARED_CLIENT_INSTANCES = "10"
  })

  after(() => {
    if (previousMaxSharedClientInstances !== undefined) {
      process.env.MAX_SHARED_CLIENT_INSTANCES = previousMaxSharedClientInstances
      return
    }
    delete process.env.MAX_SHARED_CLIENT_INSTANCES
  })

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
    const consumer = await client.declareConsumer({ stream: testStreamName, offset: Offset.first() }, () => null)

    const response = await client.closeConsumer(consumer.extendedId)

    expect(response).eql(true)
    expect(client.consumerCounts()).eql(0)
    await eventually(() => {
      const { readable } = consumer.getConnectionInfo()
      expect(readable).eql(false)
    })
  }).timeout(5000)

  it("closing a non-existing consumer should rise an error", async () => {
    const nonExistingConsumerId = computeExtendedConsumerId(123456, randomUUID())
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

    await client.closeConsumer(sharingConsumers[0].extendedId)

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

    for (const c of sharingConsumers) {
      await client.closeConsumer(c.extendedId)
    }

    await eventually(() => {
      expect(sharingConsumers).satisfies((consumerArrays: Consumer[]) =>
        consumerArrays.every((consumer) => {
          const { readable } = consumer.getConnectionInfo()
          return readable !== true
        })
      )
    })
  }).timeout(5000)

  it("if consumers for the same stream have different underlying clients, then closing one client does not affect the others consumers", async () => {
    const consumersToCreate = (getMaxSharedConnectionInstances() + 1) * (getTestNodesFromEnv().length + 1)
    const consumers = new Map<number, Consumer[]>()
    for (let i = 0; i < consumersToCreate; i++) {
      const consumer = await createConsumer(testStreamName, client)
      const { localPort } = consumer.getConnectionInfo()
      const key = localPort || -1
      const currentConsumers = consumers.get(key) || []
      currentConsumers.push(consumer)
      consumers.set(key, currentConsumers)
    }
    const localPort = Array.from(consumers.keys()).at(0)
    const closingConsumersSubset = consumers.get(localPort!) || []
    const otherConsumers: Consumer[] = []
    for (const k of consumers.keys()) {
      if (k !== localPort) {
        otherConsumers.push(...(consumers.get(k) || []))
      }
    }

    for (const c of closingConsumersSubset) {
      await client.closeConsumer(c.extendedId)
    }

    expect(localPort).not.undefined
    expect(closingConsumersSubset.length).gt(0)
    expect(otherConsumers.length).gt(0)
    expect(otherConsumers).satisfies((consumerArray: Consumer[]) =>
      consumerArray.every((consumer) => {
        const { readable } = consumer.getConnectionInfo()
        return readable === true
      })
    )
    await eventually(() => {
      expect(closingConsumersSubset).satisfies((consumerArray: Consumer[]) =>
        consumerArray.every((consumer) => {
          const { readable } = consumer.getConnectionInfo()
          return readable !== true
        })
      )
    })
  }).timeout(5000)
})
