import { expect } from "chai"
import { Connection } from "../../src"
import { getMessageFrom } from "../../src/util"
import { createConnection, createProperties, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"
import { randomUUID } from "crypto"
import { Producer } from "../../src/producer"

describe("publish a message", () => {
  const rabbit = new Rabbit()
  let connection: Connection
  let streamName: string
  let publisher: Producer

  beforeEach(async () => {
    connection = await createConnection()
    streamName = createStreamName()
    await rabbit.createStream(streamName)
    publisher = await createPublisher(streamName, connection)
  })

  afterEach(async () => {
    try {
      await connection.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("is seen by rabbit", async () => {
    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(streamName)).messages).eql(1)
    }, 10000)
  }).timeout(10000)

  it("and a lot more are all seen by rabbit", async () => {
    for (let index = 0; index < 100; index++) {
      await publisher.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
    }

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(streamName)).messages).eql(100)
    }, 10000)
  }).timeout(30000)

  it("can be read using classic client", async () => {
    const message = `test${randomUUID()}`

    await publisher.send(BigInt(Date.now() + 1), Buffer.from(message))

    const { content } = await getMessageFrom(streamName)
    expect(message).eql(content)
  })

  it("with properties and they are read from classic client", async () => {
    const message = `test${randomUUID()}`
    const properties = createProperties()

    await publisher.send(BigInt(Date.now() + 1), Buffer.from(message), { properties })

    const msg = await getMessageFrom(streamName)
    const { content, properties: classicProperties } = msg
    expect(message).eql(content)
    expect(Math.floor((properties.creationTime?.getTime() || 1) / 1000)).eql(classicProperties.timestamp)
    expect(properties.replyTo).eql(classicProperties.replyTo)
    expect(properties.correlationId).eql(classicProperties.correlationId)
    expect(properties.contentEncoding).eql(classicProperties.contentEncoding)
    expect(properties.contentType).eql(classicProperties.contentType)
    expect(properties.messageId).eql(classicProperties.messageId)
    expect(properties.userId?.toString()).eql(classicProperties.userId)
  })

  it("with application properties and they are read from classic client", async () => {
    const message = `test${randomUUID()}`
    const applicationProperties = { "my-key": "my-value", key: "value", k: 100000 }

    await publisher.send(BigInt(Date.now() + 1), Buffer.from(message), { applicationProperties })

    const msg = await getMessageFrom(streamName)
    const { content, properties } = msg
    expect(message).eql(content)
    expect(properties.headers).eql({ ...applicationProperties, "x-stream-offset": 0 })
  })

  describe("deduplication", () => {
    it("is active if create a publisher with publishRef", async () => {
      const howMany = 100
      for (let index = 0; index < howMany; index++) {
        await publisher.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
      }
      for (let index = 0; index < howMany; index++) {
        await publisher.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
      }

      await eventually(async () => expect((await rabbit.getQueueInfo(streamName)).messages).eql(howMany), 10000)
    }).timeout(30000)

    it("is not active if create a publisher with empty publisherRef", async () => {
      const publisherEmptyRef = await connection.declarePublisher({ stream: streamName, publisherRef: "" })

      const howMany = 100
      for (let index = 0; index < howMany; index++) {
        await publisherEmptyRef.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
      }
      for (let index = 0; index < howMany; index++) {
        await publisherEmptyRef.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
      }

      await eventually(async () => expect((await rabbit.getQueueInfo(streamName)).messages).eql(howMany * 2), 10000)
    }).timeout(30000)

    it("is not active if create a publisher without publishRef", async () => {
      const publisherNoRef = await connection.declarePublisher({ stream: streamName })

      const howMany = 100
      for (let index = 0; index < howMany; index++) {
        await publisherNoRef.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
      }
      for (let index = 0; index < howMany; index++) {
        await publisherNoRef.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
      }

      await eventually(async () => expect((await rabbit.getQueueInfo(streamName)).messages).eql(howMany * 2), 10000)
    }).timeout(30000)
  })
})
