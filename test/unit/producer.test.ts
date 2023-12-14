import { expect, spy, use as chaiUse } from "chai"
import { randomUUID } from "crypto"
import { Connection, connect } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"
import { FrameSizeException } from "../../src/requests/frame_size_exception"
import chaiAsPromised from "chai-as-promised"
import spies from "chai-spies"
chaiUse(chaiAsPromised)
chaiUse(spies)

describe("Producer", () => {
  const rabbit = new Rabbit(username, password)
  let testStreamName = ""
  let publisherRef: string

  beforeEach(async () => {
    testStreamName = `${randomUUID()}`
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
    const maxFrameSize = 1000
    let writeConnection: Connection | null = null
    let spySandbox: ChaiSpies.Sandbox | null = null

    beforeEach(async () => {
      spySandbox = spy.sandbox()
      writeConnection = await connect({
        hostname: "localhost",
        port: 5552,
        username,
        password,
        vhost: "/",
        frameMax: maxFrameSize,
        heartbeat: 0,
      })
    })

    afterEach(async () => {
      await writeConnection!.close()
      spySandbox?.restore()
    })
    it("if a message is too big an exception is raised when sending it", async () => {
      const publisher = await writeConnection!.declarePublisher({
        stream: testStreamName,
        publisherRef,
      })
      const msg = Buffer.from(Array.from(Array(maxFrameSize + 1).keys()).map((_v) => 1))

      return expect(publisher.send(msg, {})).to.be.rejectedWith(FrameSizeException)
    })

    it("if chunk size is not reached, then the message is enqueued", async () => {
      const chunkSize = 100
      const publisher = await writeConnection!.declarePublisher({
        stream: testStreamName,
        publisherRef,
        maxChunkLength: chunkSize,
      })
      const msg = Buffer.from([1])

      const result = await publisher.send(msg, {})

      expect(result).is.false
    })
    it("if max queue length is reached, then the chunk is sent immediately", async () => {
      const queueLength = 2
      const publisher = await writeConnection!.declarePublisher({
        stream: testStreamName,
        publisherRef,
        maxChunkLength: queueLength,
      })
      const msgs = [Buffer.from([1]), Buffer.from([2])]

      const result = await Promise.all(msgs.map((msg) => publisher.send(msg, {})))

      expect(result).eql([false, true])
    })

    it("even if max queue length is not reached, the messages are eventually sent", async () => {
      const queueLength = 10
      const messageQuantity = queueLength - 2
      const publisher = await writeConnection!.declarePublisher({
        stream: testStreamName,
        publisherRef,
        maxChunkLength: queueLength,
      })
      const msgs = Array.from(Array(messageQuantity).keys()).map((k) => Buffer.from([k]))
      spySandbox?.on(publisher, "flush")

      await Promise.all(msgs.map((msg) => publisher.send(msg, {})))

      await eventually(() => expect(publisher.flush).called)
    })
  })
})
