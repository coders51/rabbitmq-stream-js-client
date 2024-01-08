import { expect } from "chai"
import { Client } from "../../src"
import { Offset } from "../../src/requests/subscribe_request"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"

describe("subscribe", () => {
  const rabbit = new Rabbit(username, password)
  let streamName: string
  let client: Client

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("subscribe to next message", async () => {
    await eventually(async () => {
      const res = await client.subscribe({
        subscriptionId: 1,
        stream: streamName,
        offset: Offset.next(),
        credit: 0,
      })

      expect(res.ok).eql(true)
    }, 5000)
  }).timeout(10000)

  it("subscribe to first message", async () => {
    await eventually(async () => {
      const res = await client.subscribe({
        subscriptionId: 2,
        stream: streamName,
        offset: Offset.first(),
        credit: 0,
      })

      expect(res.ok).eql(true)
    }, 5000)
  }).timeout(10000)

  it("subscribe to last message", async () => {
    await eventually(async () => {
      const res = await client.subscribe({
        subscriptionId: 3,
        stream: streamName,
        offset: Offset.last(),
        credit: 0,
      })

      expect(res.ok).eql(true)
    }, 5000)
  }).timeout(10000)

  it("subscribe to offset message", async () => {
    await eventually(async () => {
      const res = await client.subscribe({
        subscriptionId: 4,
        stream: streamName,
        offset: Offset.offset(BigInt(1)),
        credit: 0,
      })

      expect(res.ok).eql(true)
    }, 5000)
  }).timeout(10000)

  it("subscribe to date message", async () => {
    await eventually(async () => {
      const res = await client.subscribe({
        subscriptionId: 5,
        stream: streamName,
        offset: Offset.timestamp(new Date()),
        credit: 0,
      })

      expect(res.ok).eql(true)
    }, 5000)
  }).timeout(10000)
})
