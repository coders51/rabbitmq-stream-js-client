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

  it("increase publishing id from server when publisherRef is defined and publishing id is not set (deduplication active)", async () => {
    const oldClient = await createClient(username, password)
    const oldPublisher = await oldClient.declarePublisher({ stream: testStreamName, publisherRef })
    const oldMessages = [...Array(3).keys()]
    await Promise.all(oldMessages.map(() => oldPublisher.send(Buffer.from(`test${randomUUID()}`))))
    await oldPublisher.flush()
    await oldClient.close()
    const newClient = await createClient(username, password)

    const newPublisher = await newClient.declarePublisher({ stream: testStreamName, publisherRef })
    await newPublisher.send(Buffer.from(`test${randomUUID()}`))
    await newPublisher.flush()

    expect(await newPublisher.getLastPublishingId()).eql(BigInt(oldMessages.length) + 1n)
    await newClient.close()
  }).timeout(10000)

  it("increase publishing id from server when publisherRef is defined and publishing id is set (deduplication active)", async () => {
    const oldClient = await createClient(username, password)
    const oldPublisher = await oldClient.declarePublisher({ stream: testStreamName, publisherRef })
    const oldMessages = [...Array(3).keys()]
    await Promise.all(
      oldMessages.map((_, index) =>
        oldPublisher.send(Buffer.from(`test${randomUUID()}`), { publishingId: BigInt(index + 1) })
      )
    )
    await oldPublisher.flush()
    await oldClient.close()
    const newClient = await createClient(username, password)

    const newPublisher = await newClient.declarePublisher({ stream: testStreamName, publisherRef })
    await newPublisher.send(Buffer.from(`test${randomUUID()}`), { publishingId: BigInt(4) })
    await newPublisher.flush()

    expect(await newPublisher.getLastPublishingId()).eql(BigInt(oldMessages.length) + 1n)
    await newClient.close()
  }).timeout(10000)

  it("should not increase publishing id when publishRef is defined and publishing two messages with same id (deduplication active)", async () => {
    const client = await createClient(username, password)
    const publisher = await client.declarePublisher({ stream: testStreamName, publisherRef })

    const publishingId = BigInt(1)
    await publisher.send(Buffer.from(`test${randomUUID()}`), { publishingId: publishingId })
    await publisher.send(Buffer.from(`test${randomUUID()}`), { publishingId: publishingId })
    await publisher.flush()

    expect(await publisher.getLastPublishingId()).eql(publishingId)
    await client.close()
  }).timeout(10000)

  it("should auto increment publishing id when publishing id is not passed from outside (deduplication active)", async () => {
    const client = await createClient(username, password)
    const publisher = await client.declarePublisher({ stream: testStreamName, publisherRef })
    await publisher.send(Buffer.from(`test${randomUUID()}`), { publishingId: 1n })
    await publisher.send(Buffer.from(`test${randomUUID()}`), { publishingId: 2n })
    await publisher.send(Buffer.from(`test${randomUUID()}`))
    await publisher.send(Buffer.from(`test${randomUUID()}`))
    await publisher.flush()

    expect(await publisher.getLastPublishingId()).eql(4n)
    await client.close()
  })

  it("should set latest publishing id when passing it from outside (deduplication active)", async () => {
    const client = await createClient(username, password)
    const publisher = await client.declarePublisher({ stream: testStreamName, publisherRef })
    await publisher.send(Buffer.from(`test${randomUUID()}`))
    await publisher.send(Buffer.from(`test${randomUUID()}`))
    await publisher.send(Buffer.from(`test${randomUUID()}`), { publishingId: 3n })
    await publisher.send(Buffer.from(`test${randomUUID()}`), { publishingId: 4n })
    await publisher.flush()

    expect(await publisher.getLastPublishingId()).eql(4n)
    await client.close()
  })

  it("do not increase publishing id from server when publisherRef is not defined (deduplication not active)", async () => {
    const oldClient = await createClient(username, password)
    const oldPublisher = await oldClient.declarePublisher({ stream: testStreamName })
    const oldMessages = [...Array(3).keys()]
    await Promise.all(oldMessages.map(() => oldPublisher.send(Buffer.from(`test${randomUUID()}`))))
    await oldPublisher.flush()
    await oldClient.close()
    const newClient = await createClient(username, password)

    const newPublisher = await newClient.declarePublisher({ stream: testStreamName })
    await newPublisher.send(Buffer.from(`test${randomUUID()}`))
    await newPublisher.flush()

    expect(await newPublisher.getLastPublishingId()).eql(BigInt(0))
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
