import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect, Connection } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually, wait } from "../support/util"

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

  it("using parameters", async () => {
    const stream = "my stream"
    await rabbit.createStream(stream)
    const publisher = await connection.declarePublisher({ stream, publisherRef: "my publisher" })

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await wait(5000)
    // await eventually(async () => {
    //   expect(await rabbit.getMessages()).lengthOf(1)
    // }, 5000)
    // await connection.close()
  }).timeout(10000)

  it("raise exception if goes in timeout")
  it("raise exception if server refuse port")
})
