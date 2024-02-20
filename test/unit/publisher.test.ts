import { use as chaiUse, expect, spy } from "chai"
import chaiAsPromised from "chai-as-promised"
import spies from "chai-spies"
import { randomUUID } from "crypto"
import { Client } from "../../src"
import { FrameSizeException } from "../../src/requests/frame_size_exception"
import { createClient } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"
chaiUse(chaiAsPromised)
chaiUse(spies)

describe("Publisher", () => {
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
    const oldClient = await createClient(username, password)
    const oldPublisher = await oldClient.declarePublisher({ stream: testStreamName, publisherRef })
    const oldMessages = [...Array(3).keys()]
    await Promise.all(oldMessages.map(() => oldPublisher.send(Buffer.from(`test${randomUUID()}`))))
    await oldPublisher.flush()
    await oldClient.close()
    const newClient = await createClient(username, password)

    const newPublisher = await newClient.declarePublisher({ stream: testStreamName, publisherRef, boot: true })
    await newPublisher.send(Buffer.from(`test${randomUUID()}`))
    await newPublisher.flush()

    expect(await newPublisher.getLastPublishingId()).eql(BigInt(oldMessages.length) + 1n)
    await newClient.close()
  }).timeout(10000)

  it("do not increase publishing id from server when boot is false", async () => {
    const oldClient = await createClient(username, password)
    const oldPublisher = await oldClient.declarePublisher({ stream: testStreamName, publisherRef })
    const oldMessages = [...Array(3).keys()]
    await Promise.all(oldMessages.map(() => oldPublisher.send(Buffer.from(`test${randomUUID()}`))))
    await oldPublisher.flush()
    await oldClient.close()
    const newClient = await createClient(username, password)

    const newPublisher = await newClient.declarePublisher({ stream: testStreamName, publisherRef, boot: false })
    await newPublisher.send(Buffer.from(`test${randomUUID()}`))

    expect(await newPublisher.getLastPublishingId()).eql(BigInt(oldMessages.length))
    await newClient.close()
  }).timeout(10000)

  describe("Send operation limits", () => {
    const maxFrameSize = 1000
    let writeClient: Client | null = null
    let spySandbox: ChaiSpies.Sandbox | null = null

    beforeEach(async () => {
      spySandbox = spy.sandbox()
      writeClient = await createClient(username, password, undefined, maxFrameSize)
    })

    afterEach(async () => {
      await writeClient!.close()
      spySandbox?.restore()
    })

    it("if a message is too big an exception is raised when sending it", async () => {
      const publisher = await writeClient!.declarePublisher({
        stream: testStreamName,
        publisherRef,
      })
      const msg = Buffer.from(Array.from(Array(maxFrameSize + 1).keys()).map((_v) => 1))

      return expect(publisher.send(msg, {})).to.be.rejectedWith(FrameSizeException)
    })

    it("if chunk size is not reached, then the message is enqueued", async () => {
      const chunkSize = 100
      const publisher = await writeClient!.declarePublisher({
        stream: testStreamName,
        publisherRef,
        maxChunkLength: chunkSize,
      })
      const msg = Buffer.from([1])

      const result = await publisher.send(msg, {})

      expect(result.sent).is.false
    })
    it("if max queue length is reached, then the chunk is sent immediately", async () => {
      const queueLength = 2
      const publisher = await writeClient!.declarePublisher({
        stream: testStreamName,
        publisherRef,
        maxChunkLength: queueLength,
      })
      const msgs = [Buffer.from([1]), Buffer.from([2])]

      const result = await Promise.all(msgs.map((msg) => publisher.send(msg, {})))

      expect(result[0].sent).is.false
      expect(result[1].sent).is.true
    })

    it("even if max queue length is not reached, the messages are eventually sent", async () => {
      const queueLength = 10
      const messageQuantity = queueLength - 2
      const publisher = await writeClient!.declarePublisher({
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
