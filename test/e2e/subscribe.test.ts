import { expect } from "chai"
<<<<<<< HEAD
import { connect } from "../../src/connection"
=======
import { connect, Connection } from "../../src/connection"
>>>>>>> ac2dd96 (add subscribe command)
import { Offset } from "../../src/requests/subscribe_request"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"

describe("subscribe", async () => {
  const rabbit = new Rabbit()
  const streamName = "test-stream"
<<<<<<< HEAD

  beforeEach(async () => {
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    await rabbit.deleteStream(streamName)
  })

  it("subscribe to next message", async () => {
    const connection = await connect({
=======
  let connection: Connection

  before(async () => {
    await rabbit.createStream(streamName)
    connection = await connect({
>>>>>>> ac2dd96 (add subscribe command)
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
<<<<<<< HEAD
      frameMax: 0,
      heartbeat: 0,
    })

=======
      frameMax: 0, // not used
      heartbeat: 0, // not used
    })
  })

  it("subscribe to next message", async () => {
>>>>>>> ac2dd96 (add subscribe command)
    const res = await connection.subscribe({
      subscriptionId: 1,
      stream: streamName,
      offset: Offset.next(),
      credit: 0,
    })

    await eventually(async () => {
      expect(res.ok).eql(true)
    }, 5000)
<<<<<<< HEAD
    await connection.close()
  }).timeout(10000)

  it("subscribe to first message", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })

=======
  })

  it("subscribe to first message", async () => {
>>>>>>> ac2dd96 (add subscribe command)
    const res = await connection.subscribe({
      subscriptionId: 2,
      stream: streamName,
      offset: Offset.first(),
      credit: 0,
    })

    await eventually(async () => {
      expect(res.ok).eql(true)
    }, 5000)
<<<<<<< HEAD
    await connection.close()
  }).timeout(10000)

  it("subscribe to last message", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })

=======
  })

  it("subscribe to last message", async () => {
>>>>>>> ac2dd96 (add subscribe command)
    const res = await connection.subscribe({
      subscriptionId: 3,
      stream: streamName,
      offset: Offset.last(),
      credit: 0,
    })

    await eventually(async () => {
      expect(res.ok).eql(true)
    }, 5000)
<<<<<<< HEAD
    await connection.close()
  }).timeout(10000)

  it("subscribe to offset message", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })

=======
  })

  it("subscribe to offset message", async () => {
>>>>>>> ac2dd96 (add subscribe command)
    const res = await connection.subscribe({
      subscriptionId: 4,
      stream: streamName,
      offset: Offset.offset(BigInt(1)),
      credit: 0,
    })

    await eventually(async () => {
      expect(res.ok).eql(true)
    }, 5000)
<<<<<<< HEAD
    await connection.close()
  }).timeout(10000)

  it("subscribe to date message", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })

=======
  })

  it("subscribe to date message", async () => {
>>>>>>> ac2dd96 (add subscribe command)
    const res = await connection.subscribe({
      subscriptionId: 5,
      stream: streamName,
      offset: Offset.timestamp(new Date()),
      credit: 0,
    })

    await eventually(async () => {
      expect(res.ok).eql(true)
    }, 5000)
<<<<<<< HEAD
    await connection.close()
  }).timeout(10000)
=======
  })
>>>>>>> ac2dd96 (add subscribe command)
})
