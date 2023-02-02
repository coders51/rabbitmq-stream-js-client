import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect } from "../../src"
import { Rabbit } from "../support/rabbit"

describe("Producer", () => {
  const rabbit = new Rabbit()
  const testStreamName = "test-stream"
  let publisherRef: string

  beforeEach(async () => {
    publisherRef = randomUUID()
    await rabbit.createStream(testStreamName)
  })

  afterEach(() => rabbit.deleteStream(testStreamName))

  it("increase publishing id from server when boot is true", async () => {
    const oldConnection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })
    const oldPublisher = await oldConnection.declarePublisher({ stream: testStreamName, publisherRef })
    const oldMessages = [...Array(3).keys()]
    await Promise.all(oldMessages.map(() => oldPublisher.send(Buffer.from(`test${randomUUID()}`))))
    await oldConnection.close()
    const newConnection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })

    const newPublisher = await newConnection.declarePublisher({ stream: testStreamName, publisherRef, boot: true })
    await newPublisher.send(Buffer.from(`test${randomUUID()}`))

    expect(await newPublisher.getLastPublishingId()).eql(BigInt(oldMessages.length) + 1n)
    await newConnection.close()
  }).timeout(10000)

  it("do not increase publishing id from server when boot is false", async () => {
    const oldConnection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })
    const oldPublisher = await oldConnection.declarePublisher({ stream: testStreamName, publisherRef })
    const oldMessages = [...Array(3).keys()]
    await Promise.all(oldMessages.map(() => oldPublisher.send(Buffer.from(`test${randomUUID()}`))))
    await oldConnection.close()
    const newConnection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })

    const newPublisher = await newConnection.declarePublisher({ stream: testStreamName, publisherRef, boot: false })
    await newPublisher.send(Buffer.from(`test${randomUUID()}`))

    expect(await newPublisher.getLastPublishingId()).eql(BigInt(oldMessages.length))
    await newConnection.close()
  }).timeout(10000)
})
