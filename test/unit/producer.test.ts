import { expect, use as chaiUse } from "chai"
import { randomUUID } from "crypto"
import { Connection, connect } from "../../src"
import { Rabbit } from "../support/rabbit"
import { password, username } from "../support/util"
import { FrameSizeException } from "../../src/requests/frame_size_exception"
import chaiAsPromised from "chai-as-promised"
chaiUse(chaiAsPromised)

describe("Producer", () => {
  const rabbit = new Rabbit(username, password)
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
      username,
      password,
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })
    const oldPublisher = await oldConnection.declarePublisher({ stream: testStreamName, publisherRef })
    const oldMessages = [...Array(3).keys()]
    await Promise.all(oldMessages.map(() => oldPublisher.send(Buffer.from(`test${randomUUID()}`))))
    await oldPublisher.flush()
    await oldConnection.close()
    const newConnection = await connect({
      hostname: "localhost",
      port: 5552,
      username,
      password,
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })

    const newPublisher = await newConnection.declarePublisher({ stream: testStreamName, publisherRef, boot: true })
    await newPublisher.send(Buffer.from(`test${randomUUID()}`))
    await newPublisher.flush()

    expect(await newPublisher.getLastPublishingId()).eql(BigInt(oldMessages.length) + 1n)
    await newConnection.close()
  }).timeout(10000)

  it("do not increase publishing id from server when boot is false", async () => {
    const oldConnection = await connect({
      hostname: "localhost",
      port: 5552,
      username,
      password,
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })
    const oldPublisher = await oldConnection.declarePublisher({ stream: testStreamName, publisherRef })
    const oldMessages = [...Array(3).keys()]
    await Promise.all(oldMessages.map(() => oldPublisher.send(Buffer.from(`test${randomUUID()}`))))
    await oldPublisher.flush()
    await oldConnection.close()
    const newConnection = await connect({
      hostname: "localhost",
      port: 5552,
      username,
      password,
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })

    const newPublisher = await newConnection.declarePublisher({ stream: testStreamName, publisherRef, boot: false })
    await newPublisher.send(Buffer.from(`test${randomUUID()}`))

    expect(await newPublisher.getLastPublishingId()).eql(BigInt(oldMessages.length))
    await newConnection.close()
  }).timeout(10000)

  describe("Send operation limits", () => {
    let connection: Connection | null = null

    beforeEach(async () => {
      connection = await connect({
        hostname: "localhost",
        port: 5552,
        username,
        password,
        vhost: "/",
        frameMax: 0,
        heartbeat: 0,
      })
    })

    afterEach(async () => {
      await connection!.close()
    })
    it("if a message is too big an exception is raised when sending it", async () => {
      const maxFrameSize = 1000
      const publisher = await connection!.declarePublisher({
        stream: testStreamName,
        publisherRef,
        maxFrameSize: maxFrameSize,
      })
      const msg = Buffer.from(Array.from(Array(maxFrameSize + 1).keys()).map((_v) => 1))

      return expect(publisher.send(msg, {})).to.be.rejectedWith(FrameSizeException)
    })

    it("if chunk size is not reached, then the message is enqueued", async () => {
      const maxFrameSize = 1000
      const chunkSize = 100
      const publisher = await connection!.declarePublisher({
        stream: testStreamName,
        publisherRef,
        maxFrameSize: maxFrameSize,
        maxChunkLength: chunkSize,
      })
      const msg = Buffer.from([1])

      const result = await publisher.send(msg, {})

      expect(result).is.false
    })
    it("if max queue length is reached, then the chunk is sent immediately", async () => {
      const maxFrameSize = 1000
      const queueLength = 2
      const publisher = await connection!.declarePublisher({
        stream: testStreamName,
        publisherRef,
        maxFrameSize: maxFrameSize,
        maxChunkLength: queueLength,
      })
      const msgs = [Buffer.from([1]), Buffer.from([2])]

      const result = await Promise.all(msgs.map((msg) => publisher.send(msg, {})))

      expect(result).eql([false, true])
    })
  })
})
