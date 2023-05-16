import { expect } from "chai"
import { randomUUID } from "crypto"
import { Connection, ListenersParams } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"
import { createConnection } from "../support/fake_data"

describe("publish a message and get confirmation", () => {
  const rabbit = new Rabbit(username, password)
  let connection: Connection
  let confirmed: boolean
  let stream: string
  let publishResponses: bigint[][] = []
  const publisherRef = "my publisher"
  const listeners: ListenersParams = {
    publish_confirm: (_data) => (confirmed = true),
  }

  beforeEach(async () => {
    connection = await createConnection(username, password, listeners)
    stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    confirmed = false
    publishResponses = []
  })
  afterEach(() => connection.close())
  afterEach(() => rabbit.closeAllConnections())

  it("after the server replies with a confirm, the confirm callback is invoked", async () => {
    const publisher = await connection.declarePublisher({ stream, publisherRef })
    const publishingId = 1n
    publisher.on("publish_confirm", (err: number | null, pubIds: bigint[]) => {
      confirmed = !err
      publishResponses.push(pubIds)
    })

    await publisher.send(publishingId, Buffer.from(`test${randomUUID()}`))

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
      expect(publishResponses[0]).to.include(1n)
      expect(confirmed).true
    }, 5000)
  }).timeout(10000)

  it("after the server replies with a confirm, the confirm callback is invoked with the publishingId as an argument", async () => {
    let publishingIds: bigint[] = []
    const publisher = await connection.declarePublisher({ stream, publisherRef })
    publisher.on("publish_confirm", (_err: number | null, pubIds: bigint[]) => (publishingIds = pubIds))

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))
    const lastPublishingId = await publisher.getLastPublishingId()

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
    }, 10000)
    expect(publishingIds.slice(-1).pop()).equals(lastPublishingId)
  }).timeout(10000)

  it("after the server replies with an error, the error callback is invoked", async () => {
    let errored = false
    const publisher = await connection.declarePublisher({ stream, publisherRef })
    publisher.on("publish_confirm", (error, _publishingIds) => {
      if (error) {
        errored = true
      }
    })
    await rabbit.deleteStream(stream)

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(() => expect(errored).to.be.true)
  }).timeout(10000)

  it("after the server replies with an error, the error callback is invoked with the error as an argument", async () => {
    let error: number | undefined = undefined
    const publisher = await connection.declarePublisher({ stream, publisherRef })
    publisher.on("publish_confirm", (err, _publishingIds) => {
      if (err) {
        error = err
      }
    })
    await rabbit.deleteStream(stream)

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(() => {
      expect(error).gt(0)
    })
  }).timeout(10000)
})
