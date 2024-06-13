import { expect } from "chai"
import { Client, Publisher } from "../../src"
import { Rabbit } from "../support/rabbit"
import { password, username } from "../support/util"
import { createClient, createPublisher } from "../support/fake_data"
import { getMaxSharedConnectionInstances } from "../../src/util"

describe("close publisher", () => {
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

  it("closing a publisher", async () => {
    const publisher = await client.declarePublisher({ stream: testStreamName })

    const response = await client.deletePublisher(publisher.extendedId)

    const publisherInfo = publisher.getConnectionInfo()
    expect(response).eql(true)

    expect(publisherInfo.writable).eql(false)
  }).timeout(5000)

  it("closing a publisher does not close the underlying connection if it is still in use", async () => {
    const publisher1 = await createPublisher(testStreamName, client)
    const publisher2 = await createPublisher(testStreamName, client)

    await client.deletePublisher(publisher1.extendedId)

    const publisher2Info = publisher2.getConnectionInfo()
    expect(publisher2Info.writable).eql(true)
  })

  it("closing all publishers sharing a connection also close the connection", async () => {
    const publisher1 = await createPublisher(testStreamName, client)
    const publisher2 = await createPublisher(testStreamName, client)

    await client.deletePublisher(publisher1.extendedId)
    await client.deletePublisher(publisher2.extendedId)

    const publisher1Info = publisher1.getConnectionInfo()
    const publisher2Info = publisher2.getConnectionInfo()
    expect(publisher1Info.writable).eql(false)
    expect(publisher2Info.writable).eql(false)
  })

  it("if publishers for the same stream have different underlying clients, then closing one client does not affect the others publishers", async () => {
    const publishersToCreate = getMaxSharedConnectionInstances() + 2
    const publishers = new Map<number, Publisher[]>()
    for (let i = 0; i < publishersToCreate; i++) {
      const publisher = await createPublisher(testStreamName, client)
      const { localPort } = publisher.getConnectionInfo()
      const key = localPort || -1
      const currentPublishers = publishers.get(key) || []
      currentPublishers.push(publisher)
      publishers.set(key, currentPublishers)
    }
    const localPort = Array.from(publishers.keys()).at(0)
    const closingPublishersSubset = publishers.get(localPort!) || []
    const otherPublishers: Publisher[] = []
    for (const k of publishers.keys()) {
      if (k !== localPort) {
        otherPublishers.push(...(publishers.get(k) || []))
      }
    }

    for (const p of closingPublishersSubset) {
      await client.deletePublisher(p.extendedId)
    }

    expect(localPort).not.undefined
    expect(closingPublishersSubset.length).gt(0)
    expect(otherPublishers.length).gt(0)
    expect(otherPublishers).satisfies((publisherArray: Publisher[]) =>
      publisherArray.every((publisher) => {
        const { writable } = publisher.getConnectionInfo()
        return writable === true
      })
    )
    expect(closingPublishersSubset).satisfies((publisherArray: Publisher[]) =>
      publisherArray.every((publisher) => {
        const { writable } = publisher.getConnectionInfo()
        return writable !== true
      })
    )
  }).timeout(5000)
})
