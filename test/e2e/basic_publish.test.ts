import { expect } from "chai"
import { randomUUID } from "crypto"
import { Client, Offset, Publisher } from "../../src"
import { createClient, createProperties, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password, getMessageFrom } from "../support/util"
import { BufferSizeSettings } from "../../src/requests/request"
import { FrameSizeException } from "../../src/requests/frame_size_exception"
import { Message, MessageProperties } from "../../src/publisher"

describe("publish a message", () => {
  const rabbit = new Rabbit(username, password)
  let client: Client
  let streamName: string
  let publisher: Publisher
  let bufferSizeSettings: BufferSizeSettings | undefined = undefined
  let maxFrameSize: number | undefined = undefined

  beforeEach(async () => {
    client = await createClient(username, password, undefined, maxFrameSize, bufferSizeSettings)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
    publisher = await createPublisher(streamName, client)
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("is seen by rabbit", async () => {
    await publisher.send(Buffer.from(`test${randomUUID()}`))

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(streamName)).messages).eql(1)
    }, 10000)
  }).timeout(10000)

  it("and a lot more are all seen by rabbit", async () => {
    for (let index = 0; index < 100; index++) {
      await publisher.send(Buffer.from(`test${randomUUID()}`))
    }

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(streamName)).messages).eql(100)
    }, 10000)
  }).timeout(30000)

  it("can be read using classic client", async () => {
    const message = `test${randomUUID()}`

    await publisher.send(Buffer.from(message))

    const { content } = await getMessageFrom(streamName, username, password)
    expect(message).eql(content)
  })

  it("with properties and they are read from classic client", async () => {
    const message = `test${randomUUID()}`
    const messageProperties = createProperties()

    await publisher.send(Buffer.from(message), { messageProperties })

    const msg = await getMessageFrom(streamName, username, password)
    const { content, properties: classicProperties } = msg
    expect(message).eql(content)
    expect(Math.floor((messageProperties.creationTime?.getTime() || 1) / 1000)).eql(classicProperties.timestamp)
    expect(messageProperties.replyTo).eql(classicProperties.replyTo)
    expect(messageProperties.correlationId).eql(classicProperties.correlationId)
    expect(messageProperties.contentEncoding).eql(classicProperties.contentEncoding)
    expect(messageProperties.contentType).eql(classicProperties.contentType)
    expect(messageProperties.messageId).eql(classicProperties.messageId)
    expect(messageProperties.userId?.toString()).eql(classicProperties.userId)
  })

  it("publish with partial properties", async () => {
    const messageContent = `test${randomUUID()}`
    let message: Message | undefined = undefined
    const messageProperties: MessageProperties = { messageId: "test", subject: "test" }
    await client.declareConsumer({ stream: streamName, offset: Offset.first() }, (msg) => {
      message = msg
    })

    await publisher.send(Buffer.from(messageContent), { messageProperties })

    await eventually(() => {
      expect(message).not.to.be.undefined
      expect(messageContent).eql(message!.content.toString())
      expect(messageProperties.subject).eql(message?.messageProperties?.subject)
      expect(messageProperties.messageId).eql(message?.messageProperties?.messageId)
    })
  })

  it("with application properties and they are read from classic client", async () => {
    const message = `test${randomUUID()}`
    const applicationProperties = { "my-key": "my-value", key: "value", k: 100000 }

    await publisher.send(Buffer.from(message), { applicationProperties })

    const msg = await getMessageFrom(streamName, username, password)
    const { content, properties } = msg
    expect(message).eql(content)
    expect(properties.headers).eql({ ...applicationProperties, "x-stream-offset": 0 })
  })

  describe("with custom buffer size limits", () => {
    before(() => {
      bufferSizeSettings = { initialSize: 16 }
      maxFrameSize = 256
    })

    after(() => {
      bufferSizeSettings = undefined
      maxFrameSize = undefined
    })

    it("send a message that triggers buffer size growth", async () => {
      const message = Array(bufferSizeSettings!.initialSize! * 3).join(".")

      await publisher.send(Buffer.from(message))

      const msg = await getMessageFrom(streamName, username, password)
      const { content } = msg
      expect(message).eql(content)
    })

    it("max buffer size reached, exception thrown", async () => {
      const message = Array(maxFrameSize).join(".")

      return expect(publisher.send(Buffer.from(message))).to.be.rejectedWith(FrameSizeException)
    })
  })

  describe("deduplication", () => {
    it("is active if create a publisher with publishRef", async () => {
      const howMany = 100
      for (let index = 0; index < howMany; index++) {
        await publisher.basicSend(BigInt(index), Buffer.from(`test${randomUUID()}`))
      }
      for (let index = 0; index < howMany; index++) {
        await publisher.basicSend(BigInt(index), Buffer.from(`test${randomUUID()}`))
      }

      await eventually(async () => expect((await rabbit.getQueueInfo(streamName)).messages).eql(howMany), 10000)
    }).timeout(30000)

    it("is not active if create a publisher with empty publisherRef", async () => {
      const publisherEmptyRef = await client.declarePublisher({ stream: streamName, publisherRef: "" })

      const howMany = 100
      for (let index = 0; index < howMany; index++) {
        await publisherEmptyRef.send(Buffer.from(`test${randomUUID()}`))
      }
      for (let index = 0; index < howMany; index++) {
        await publisherEmptyRef.send(Buffer.from(`test${randomUUID()}`))
      }

      await eventually(async () => expect((await rabbit.getQueueInfo(streamName)).messages).eql(howMany * 2), 10000)
    }).timeout(30000)

    it("is not active if create a publisher without publishRef", async () => {
      const publisherNoRef = await client.declarePublisher({ stream: streamName })

      const howMany = 100
      for (let index = 0; index < howMany; index++) {
        await publisherNoRef.send(Buffer.from(`test${randomUUID()}`))
      }
      for (let index = 0; index < howMany; index++) {
        await publisherNoRef.send(Buffer.from(`test${randomUUID()}`))
      }

      await eventually(async () => expect((await rabbit.getQueueInfo(streamName)).messages).eql(howMany * 2), 10000)
    }).timeout(30000)
  })
})
