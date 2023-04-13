import { expect } from "chai"
import { Offset } from "../../src/requests/subscribe_request"
import { createConnection, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"
import { Connection } from "../../src/connection"

describe("subscribe", () => {
  let streamName: string
  let connection: Connection
  const rabbit = new Rabbit("rabbit", "rabbit")

  beforeEach(async () => {
    connection = await createConnection()
    streamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    try {
      await connection.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("subscribe to next message", async () => {
    const res = await connection.subscribe({
      subscriptionId: 1,
      stream: streamName,
      offset: Offset.next(),
      credit: 0,
    })

    await eventually(async () => {
      expect(res.ok).eql(true)
    }, 5000)
  }).timeout(10000)

  it("subscribe to first message", async () => {
    const res = await connection.subscribe({
      subscriptionId: 2,
      stream: streamName,
      offset: Offset.first(),
      credit: 0,
    })

    await eventually(async () => {
      expect(res.ok).eql(true)
    }, 5000)
  }).timeout(10000)

  it("subscribe to last message", async () => {
    const res = await connection.subscribe({
      subscriptionId: 3,
      stream: streamName,
      offset: Offset.last(),
      credit: 0,
    })

    await eventually(async () => {
      expect(res.ok).eql(true)
    }, 5000)
  }).timeout(10000)

  it("subscribe to offset message", async () => {
    const res = await connection.subscribe({
      subscriptionId: 4,
      stream: streamName,
      offset: Offset.offset(BigInt(1)),
      credit: 0,
    })

    await eventually(async () => {
      expect(res.ok).eql(true)
    }, 5000)
  }).timeout(10000)

  it("subscribe to date message", async () => {
    const res = await connection.subscribe({
      subscriptionId: 5,
      stream: streamName,
      offset: Offset.timestamp(new Date()),
      credit: 0,
    })

    await eventually(async () => {
      expect(res.ok).eql(true)
    }, 5000)
  }).timeout(10000)
})
