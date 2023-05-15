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
  const publisherRef = "my publisher"
  const listeners: ListenersParams = {
    metadata_update: (_data) => {
      return
    },
    publish_confirm: (_data) => (confirmed = true),
  }

  beforeEach(async () => {
    connection = await createConnection(username, password, listeners)
    stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    confirmed = false
  })
  afterEach(() => connection.close())
  afterEach(() => rabbit.closeAllConnections())

  it("after the server replies with a confirm, the confirm callback is invoked", async () => {
    const publisher = await connection.declarePublisher({ stream, publisherRef })
    publisher.on("publish_confirm", (err: Error | null, _pubIds: bigint[]) => (confirmed = !err))

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
      expect(confirmed).true
    }, 5000)
  }).timeout(10000)

  it("after the server replies with a confirm, the confirm callback is invoked with the publishingId as an argument", async () => {
    let publishingIds: bigint[] = []
    const publisher = await connection.declarePublisher({ stream, publisherRef })
    publisher.on("publish_confirm", (_err: Error | null, pubIds: bigint[]) => (publishingIds = pubIds))

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))
    const lastPublishingId = await publisher.getLastPublishingId()

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
    }, 10000)
    expect(publishingIds.slice(-1).pop()).equals(lastPublishingId)
  }).timeout(10000)

  it.skip("after the server replies with an error, the error callback is invoked", async () => {
    // how to force an error from the server? --LM
    let errored = false
    const publisher = await connection.declarePublisher({ stream, publisherRef })
    publisher.on("publish_confirm", (error, _publishingIds) => {
      if (error) {
        errored = true
      }
    })

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
    }, 10000)
    expect(errored).true
  }).timeout(10000)

  it.skip(
    "after the server replies with an error, the error callback is invoked with the error as an argument",
    async () => {
      // how to force an error from the server? --LM
      let error: Error | undefined = undefined
      const publisher = await connection.declarePublisher({ stream, publisherRef })
      publisher.on("publish_confirm", (err, _publishingIds) => {
        if (err) {
          error = err
        }
      })

      await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

      await eventually(async () => {
        expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
      }, 10000)
      console.error(error)
    }
  ).timeout(10000)
})
