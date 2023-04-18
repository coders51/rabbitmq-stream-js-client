import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect, Connection } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"

describe("publish a message and get confirmation", () => {
  const rabbit = new Rabbit()
  let connection: Connection
  let confirmed: boolean
  let stream: string
  const publisherRef = "my publisher"

  beforeEach(async () => {
    connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0, // not used
      heartbeat: 0, // not used
      listeners: {
        metadata_update: (_data) => {
          return
        },
        publish_confirm: (_data) => (confirmed = true),
      },
    })
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
