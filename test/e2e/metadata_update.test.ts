import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect, Connection } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"

describe.skip("update the metadata from the server", () => {
  const rabbit = new Rabbit()
  let connection: Connection
  let connection2: Connection

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
    connection2 = await connect({
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

  it("when a new publisher connects to the stream metadataupdate is called", async () => {
    const stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    let called = 0
    const publisher = await connection.declarePublisher({ stream, publisherRef: "my publisher" })
    connection.on("metadataupdate", (_) => called++)
    await connection2.declarePublisher({ stream, publisherRef: "my publisher" })
    await connection2.close()

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
    }, 10000)
    await connection.close()
  }).timeout(10000)

  // it("and a lot more are all seen by rabbit", async () => {
  //   const stream = `my-stream-${randomUUID()}`
  //   await rabbit.createStream(stream)
  //   const publisher = await connection.declarePublisher({ stream, publisherRef: "my publisher" })

  //   for (let index = 0; index < 100; index++) {
  //     await publisher.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
  //   }

  //   await eventually(async () => {
  //     expect((await rabbit.getQueueInfo(stream)).messages).eql(100)
  //   }, 10000)
  //   await connection.close()
  // }).timeout(30000)
})
