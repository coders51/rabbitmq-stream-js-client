import { expect } from "chai"
import { randomUUID } from "crypto"
import { Connection } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"
import { createConnection } from "../support/fake_data"

describe("publish a message and get confirmation", () => {
  const rabbit = new Rabbit(username, password)
  let connection: Connection
  let stream: string
  const publishResponses: { error: number | null; ids: bigint[] }[] = []
  const publisherRef = "my publisher"

  beforeEach(async () => {
    connection = await createConnection(username, password)
    stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    publishResponses.splice(0)
  })
  afterEach(() => connection.close())
  afterEach(() => rabbit.closeAllConnections())

  it("after the server replies with a confirm, the confirm callback is invoked", async () => {
    const publisher = await connection.declarePublisher({ stream, publisherRef })
    const publishingId = 1n
    publisher.on("publish_confirm", (error, ids) => publishResponses.push({ error, ids }))

    await publisher.send(publishingId, Buffer.from(`test${randomUUID()}`))

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
      expect(publishResponses).eql([{ error: null, ids: [publishingId] }])
    }, 5000)
  }).timeout(10000)

  it("after the server replies with a confirm, the confirm callback is invoked with the publishingId as an argument", async () => {
    const publisher = await connection.declarePublisher({ stream, publisherRef })
    publisher.on("publish_confirm", (error, ids) => publishResponses.push({ error, ids }))

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(async () => expect((await rabbit.getQueueInfo(stream)).messages).eql(1), 10000)
    const lastPublishingId = await publisher.getLastPublishingId()
    expect(publishResponses).eql([{ error: null, ids: [lastPublishingId] }])
  }).timeout(10000)

  it("after the server replies with an error, the error callback is invoked with an error", async () => {
    const publisher = await connection.declarePublisher({ stream, publisherRef })
    publisher.on("publish_confirm", (error, ids) => publishResponses.push({ error, ids }))
    await rabbit.deleteStream(stream)

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(() => expect(publishResponses).eql([{ error: 256, ids: [undefined] }]))
  }).timeout(10000)
})
