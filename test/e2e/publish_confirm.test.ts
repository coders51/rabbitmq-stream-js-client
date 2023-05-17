import { expect } from "chai"
import { randomUUID } from "crypto"
import { Connection } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"
import { createConnection } from "../support/fake_data"

describe("publish a message and get confirmation", () => {
  const rabbit = new Rabbit(username, password)
  let connection: Connection
  let confirmed: boolean
  let stream: string
  const publishResponses: bigint[][] = []
  const publisherRef = "my publisher"

  beforeEach(async () => {
    connection = await createConnection(username, password)
    stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    confirmed = false
    publishResponses.splice(0)
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

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
    }, 10000)
    const lastPublishingId = await publisher.getLastPublishingId()
    expect(publishingIds.pop()).equals(lastPublishingId)
  }).timeout(10000)

  it("after the server replies with an error, the error callback is invoked", async () => {
    const errors: number[] = []
    const publisher = await connection.declarePublisher({ stream, publisherRef })
    publisher.on("publish_confirm", (error, _publishingIds) => {
      if (error) errors.push(error)
    })
    await rabbit.deleteStream(stream)

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(() => expect(errors).lengthOf(1))
    await eventually(() => expect(errors).eql([256]))
  }).timeout(10000)

  it("after the server replies with an error, the error callback is invoked with the error as an argument", async () => {
    const errors: number[] = []
    const publisher = await connection.declarePublisher({ stream, publisherRef })
    publisher.on("publish_confirm", (err, _publishingIds) => {
      if (err) errors.push(err)
    })
    await rabbit.deleteStream(stream)

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(() => expect(errors).lengthOf(1))
    await eventually(() => expect(errors).eql([256]))
  }).timeout(10000)
})
