import { expect } from "chai"
import { Client } from "../../src"
import { createClient, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit, RabbitConnectionResponse } from "../support/rabbit"
import { eventually, expectToThrowAsync, username, password, wait } from "../support/util"
import { getMaxSharedConnectionInstances } from "../../src/util"
import { randomUUID } from "crypto"

describe("declare publisher", () => {
  let streamName: string
  let nonExistingStreamName: string
  const rabbit = new Rabbit(username, password)
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
    client = await createClient(username, password)
    streamName = createStreamName()
    nonExistingStreamName = createStreamName()
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

  it("declaring a publisher on an existing stream - the publisher should be created", async () => {
    const publisher = await createPublisher(streamName, client)

    await eventually(async () => {
      expect(await rabbit.returnPublishers(streamName))
        .lengthOf(1)
        .and.to.include(publisher.ref)
    }, 5000)
  }).timeout(10000)

  it("declaring a publisher on an existing stream with no publisherRef - the publisher should be created", async () => {
    const publisher = await createPublisher(streamName, client)

    await eventually(async () => {
      expect(await rabbit.returnPublishers(streamName))
        .lengthOf(1)
        .and.to.include(publisher.ref)
    }, 5000)
  }).timeout(10000)

  it("declaring a publisher on a non-existing stream should raise an error", async () => {
    await expectToThrowAsync(
      () => createPublisher(nonExistingStreamName, client),
      Error,
      "Stream was not found on any node"
    )
  })

  it("if the server deletes the stream, the publisher gets closed", async () => {
    const publisher = await createPublisher(streamName, client)
    await rabbit.deleteStream(streamName)
    await wait(500)

    await expectToThrowAsync(
      () => publisher.send(Buffer.from(`test${randomUUID()}`)),
      Error,
      "Publisher has been closed"
    )
  })

  it("publishers for the same stream should share the underlying connection", async () => {
    const publisher1 = await createPublisher(streamName, client)
    const publisher2 = await createPublisher(streamName, client)
    const { localPort: localPort1 } = publisher1.getConnectionInfo()
    const { localPort: localPort2 } = publisher2.getConnectionInfo()

    expect(localPort1).not.undefined
    expect(localPort2).not.undefined
    expect(localPort1).eq(localPort2)
  })

  it("if a large number of publishers for the same stream is declared, eventually a new client is instantiated even for the same stream/node", async () => {
    const publishersToCreate = getMaxSharedConnectionInstances() + 2
    const counts = new Map<string, number>()
    for (let i = 0; i < publishersToCreate; i++) {
      const publisher = await createPublisher(streamName, client)
      const { id } = publisher.getConnectionInfo()
      counts.set(id, (counts.get(id) || 0) + 1)
    }

    const countPublishersOverLimit = Array.from(counts.entries()).find(
      ([_id, count]) => count > getMaxSharedConnectionInstances()
    )
    expect(countPublishersOverLimit).is.undefined
    expect(Array.from(counts.keys()).length).gt(1)
  }).timeout(10000)

  it("declaring more than 256 publishers should not throw but rather open up multiple connections", async () => {
    const publishersToCreate = 257
    const counts = new Map<string, number>()
    for (let i = 0; i < publishersToCreate; i++) {
      const publisher = await createPublisher(streamName, client)
      const { id } = publisher.getConnectionInfo()
      counts.set(id, (counts.get(id) || 0) + 1)
    }

    expect(Array.from(counts.keys()).length).gt(1)
  }).timeout(10000)

  it("on a new connection, publisherId restarts from 0", async () => {
    const publishersToCreate = getMaxSharedConnectionInstances() + 1
    const publisherIds: number[] = []
    for (let i = 0; i < publishersToCreate; i++) {
      const publisher = await createPublisher(streamName, client)
      publisherIds.push(publisher.publisherId)
    }

    expect(publisherIds.filter((id) => id === 0).length).gt(1)
  }).timeout(10000)

  describe("when the client declares a named connection", () => {
    let connectionName: string | undefined = undefined

    beforeEach(async () => {
      try {
        await client.close()
        connectionName = `publisher-${randomUUID()}`
        client = await createClient(username, password, undefined, undefined, undefined, undefined, connectionName)
      } catch (e) {}
    })
    it("the name is inherited on the publisher connection", async () => {
      await createPublisher(streamName, client)

      await eventually(async () => {
        const connections = await rabbit.getConnections()
        expect(connections.length).eql(2)
        expect(connections).to.satisfy((conns: RabbitConnectionResponse[]) => {
          return conns.every((conn) => conn.client_properties?.connection_name === connectionName)
        })
      }, 5000)
    }).timeout(10000)
  })
})
