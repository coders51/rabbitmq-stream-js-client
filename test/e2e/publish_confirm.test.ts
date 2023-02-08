import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect, Connection } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"

describe("publish a message and get confirmation", () => {
  const rabbit = new Rabbit()
  let connection: Connection

  beforeEach(async () => {
    connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0, // not used
      heartbeat: 0, // not used
    })
  })
  afterEach(() => connection.close())
  afterEach(() => rabbit.closeAllConnections())

  it("after the server replies with a confirm, the confirm callback is invoked", async () => {
    const stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    let confirmed = false
    const publisher = await connection.declarePublisher({
      stream,
      publisherRef: "my publisher",
      confirmCb: () => {
        confirmed = true
      },
    })

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
    }, 10000)
    expect(confirmed).true
  }).timeout(10000)

  it("after the server replies with a confirm, the confirm callback is invoked with the publishingId as an argument", async () => {
    const stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    let publishingIds: bigint[] = []
    const publisher = await connection.declarePublisher({
      stream,
      publisherRef: "my publisher",
      confirmCb: (pubId: bigint[]) => {
        publishingIds = pubId
      },
    })

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))
    const lastPublishingId = publisher.getLastPublishingId()

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
    }, 10000)
    expect(publishingIds.slice(-1)).equals(lastPublishingId)
  }).timeout(10000)

  it("after the server replies with an error, the error callback is invoked", async () => {
    // how to force an error from the server? --Luca
    const stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    let errored = false
    const publisher = await connection.declarePublisher({
      stream,
      publisherRef: "my publisher",
      errorCb: () => {
        errored = true
      },
    })

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
    }, 10000)
    expect(errored).true
  }).timeout(10000)

  it("after the server replies with an error, the error callback is invoked with the error as an argument", async () => {
    // how to force an error from the server? --Luca
    const stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    let error: string = ""
    const publisher = await connection.declarePublisher({
      stream,
      publisherRef: "my publisher",
      errorCb: (e: string) => {
        error = e
      },
    })

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
    }, 10000)
    console.log(error)
  }).timeout(10000)
})
