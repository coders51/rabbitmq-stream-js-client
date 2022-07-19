import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect, Connection } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"

describe("publish a message", () => {
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

  it("is seen by rabbit", async () => {
    const stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    const publisher = await connection.declarePublisher({ stream, publisherRef: "my publisher" })

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
    }, 10000)
    await connection.close()
  }).timeout(10000)

  it("and a lot more are all seen by rabbit", async () => {
    const stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    const publisher = await connection.declarePublisher({ stream, publisherRef: "my publisher" })

    for (let index = 0; index < 100; index++) {
      await publisher.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
    }

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(100)
    }, 10000)
    await connection.close()
  }).timeout(30000)
})
