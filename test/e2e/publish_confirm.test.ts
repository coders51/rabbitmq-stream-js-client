import { expect } from "chai"
import { randomUUID } from "crypto"
import { Client } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"
import { createClient } from "../support/fake_data"

describe("publish a message and get confirmation", () => {
  const rabbit = new Rabbit(username, password)
  let client: Client
  let stream: string
  const publishResponses: { error: number | null; ids: bigint[] }[] = []
  const publisherRef = "my publisher"

  beforeEach(async () => {
    client = await createClient(username, password)
    stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    publishResponses.splice(0)
  })
  afterEach(async () => await client.close())
  afterEach(() => rabbit.closeAllConnections())

  it("after the server replies with a confirm, the confirm callback is invoked", async () => {
    const publisher = await client.declarePublisher({ stream, publisherRef })
    const publishingId = 1n
    publisher.on("publish_confirm", (error, ids) => publishResponses.push({ error, ids }))

    await publisher.basicSend(publishingId, Buffer.from(`test${randomUUID()}`))

    await eventually(async () => expect((await rabbit.getQueueInfo(stream)).messages).eql(1), 10000)
    expect(publishResponses).eql([{ error: null, ids: [publishingId] }])
  }).timeout(10000)

  it("after the server replies with a confirm, the confirm callback is invoked with the publishingId as an argument", async () => {
    const publisher = await client.declarePublisher({ stream, publisherRef })
    publisher.on("publish_confirm", (error, ids) => publishResponses.push({ error, ids }))

    await publisher.send(Buffer.from(`test${randomUUID()}`))

    await eventually(async () => expect((await rabbit.getQueueInfo(stream)).messages).eql(1), 10000)
    const lastPublishingId = await publisher.getLastPublishingId()
    expect(publishResponses).eql([{ error: null, ids: [lastPublishingId] }])
  }).timeout(12000)
})
