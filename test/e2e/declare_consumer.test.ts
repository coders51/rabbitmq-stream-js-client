import { expect } from "chai"
import { randomUUID } from "crypto"
import { readFileSync } from "fs"
import path from "path"
import { Client, Publisher } from "../../src"
import {
  AmqpByte,
  Message,
  MessageAnnotations,
  MessageApplicationProperties,
  MessageHeader,
  MessageProperties,
} from "../../src/publisher"
import { Offset } from "../../src/requests/subscribe_request"
import { BufferDataReader } from "../../src/response_decoder"
import { getMaxSharedConnectionInstances, range } from "../../src/util"
import {
  createClient,
  createConsumer,
  createConsumerRef,
  createPublisher,
  createStreamName,
} from "../support/fake_data"
import { Rabbit, RabbitConnectionResponse } from "../support/rabbit"
import {
  decodeMessageTesting,
  eventually,
  expectToThrowAsync,
  getTestNodesFromEnv,
  password,
  username,
} from "../support/util"

describe("declare consumer", () => {
  let streamName: string
  let nonExistingStreamName: string
  const rabbit = new Rabbit(username, password)
  let client: Client
  let publisher: Publisher
  const previousMaxSharedClientInstances = process.env.MAX_SHARED_CLIENT_INSTANCES

  before(() => {
    process.env.MAX_SHARED_CLIENT_INSTANCES = "10"
  })

  after(() => {
    if (previousMaxSharedClientInstances !== undefined) {
      process.env.MAX_SHARED_CLIENT_INSTANCES = previousMaxSharedClientInstances
      return
    }
    delete process.env.MAX_SHARED_CLIENT_INSTANCES
  })

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    nonExistingStreamName = createStreamName()
    await rabbit.createStream(streamName)
    publisher = await createPublisher(streamName, client)
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (_e) {}
  })

  it("declaring a consumer on an existing stream - the consumer should handle the message", async () => {
    const messages: Buffer[] = []
    await publisher.send(Buffer.from("hello"))

    await client.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) =>
      messages.push(message.content)
    )

    await eventually(() => expect(messages).eql([Buffer.from("hello")]))
  }).timeout(10000)

  it("declaring multiple active consumers on an existing stream - only one consumer should handle the message", async () => {
    const messages: Buffer[] = []
    const consumerRef = createConsumerRef()

    await publisher.send(Buffer.from("hello"))
    await client.declareConsumer(
      { stream: streamName, offset: Offset.first(), singleActive: true, consumerRef: consumerRef },
      (message: Message) => messages.push(message.content)
    )
    await client.declareConsumer(
      { stream: streamName, offset: Offset.first(), singleActive: true, consumerRef: consumerRef },
      (message: Message) => messages.push(message.content)
    )
    await client.declareConsumer(
      { stream: streamName, offset: Offset.first(), singleActive: true, consumerRef: consumerRef },
      (message: Message) => messages.push(message.content)
    )

    await eventually(() => expect(messages).eql([Buffer.from("hello")]))
  }).timeout(10000)

  it("declaring a single active consumer on an existing stream and a simple one - the active of the group and the simple should handle the message", async () => {
    const messages: Buffer[] = []
    const consumerRef = createConsumerRef()

    await publisher.send(Buffer.from("hello"))
    await client.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) =>
      messages.push(message.content)
    )
    await client.declareConsumer(
      { stream: streamName, offset: Offset.first(), singleActive: true, consumerRef: consumerRef },
      (message: Message) => messages.push(message.content)
    )
    await client.declareConsumer(
      { stream: streamName, offset: Offset.first(), singleActive: true, consumerRef: consumerRef },
      (message: Message) => messages.push(message.content)
    )

    await eventually(() => expect(messages).eql([Buffer.from("hello"), Buffer.from("hello")]))
  }).timeout(10000)

  it("declaring two single active consumer group on an existing stream - the active of the groups should handle the message", async () => {
    const messages: Buffer[] = []
    const consumerRef = createConsumerRef()
    const consumerRef1 = createConsumerRef()

    await publisher.send(Buffer.from("hello"))
    await client.declareConsumer(
      { stream: streamName, offset: Offset.first(), singleActive: true, consumerRef: consumerRef },
      (message: Message) => messages.push(message.content)
    )
    await client.declareConsumer(
      { stream: streamName, offset: Offset.first(), singleActive: true, consumerRef: consumerRef },
      (message: Message) => messages.push(message.content)
    )
    await client.declareConsumer(
      { stream: streamName, offset: Offset.first(), singleActive: true, consumerRef: consumerRef1 },
      (message: Message) => messages.push(message.content)
    )
    await client.declareConsumer(
      { stream: streamName, offset: Offset.first(), singleActive: true, consumerRef: consumerRef1 },
      (message: Message) => messages.push(message.content)
    )

    await eventually(() => expect(messages).eql([Buffer.from("hello"), Buffer.from("hello")]))
  }).timeout(10000)

  it("declaring a single active consumer without reference on an existing stream - should throw an error", async () => {
    const messages: Buffer[] = []

    await publisher.send(Buffer.from("hello"))

    await expectToThrowAsync(
      async () => {
        await client.declareConsumer(
          { stream: streamName, offset: Offset.first(), singleActive: true },
          (message: Message) => messages.push(message.content)
        )
      },
      Error,
      "consumerRef is mandatory when declaring a single active consumer"
    )
  }).timeout(10000)

  it("declaring a consumer on an existing stream - the consumer should handle more then one message", async () => {
    const messages: Buffer[] = []
    await publisher.send(Buffer.from("hello"))
    await publisher.send(Buffer.from("world"))
    await publisher.send(Buffer.from("world"))

    await client.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) =>
      messages.push(message.content)
    )

    await eventually(() => expect(messages).eql([Buffer.from("hello"), Buffer.from("world"), Buffer.from("world")]))
  }).timeout(10000)

  it(`consume a lot of messages`, async () => {
    const receivedMessages: Buffer[] = []
    await client.declareConsumer({ stream: streamName, offset: Offset.next() }, (message: Message) => {
      receivedMessages.push(message.content)
    })

    const messages = range(3000).map((n) => Buffer.from(`hello${n}`))
    for (const m of messages) {
      await publisher.send(m)
    }

    await eventually(() => expect(receivedMessages).eql(messages), 10000)
  }).timeout(50000)

  it("declaring a consumer on a non-existing stream should raise an error", async () => {
    await expectToThrowAsync(
      () => client.declareConsumer({ stream: nonExistingStreamName, offset: Offset.first() }, () => null),
      Error,
      "Stream was not found on any node"
    )
  })

  it("declaring a consumer on an existing stream - the consumer should read message properties", async () => {
    const messageProperties: MessageProperties[] = []
    const properties = createProperties()
    await publisher.send(Buffer.from("hello"), { messageProperties: properties })

    await client.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) => {
      messageProperties.push(message.messageProperties || {})
    })

    await eventually(async () => {
      expect(messageProperties).eql([properties])
    })
  }).timeout(10000)

  it("declaring a consumer on an existing stream - the consumer should read application properties", async () => {
    const messageApplicationProperties: MessageApplicationProperties[] = []
    const applicationProperties = createApplicationProperties()
    await publisher.send(Buffer.from("hello"), { applicationProperties })

    await client.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) => {
      messageApplicationProperties.push(message.applicationProperties || {})
    })

    await eventually(async () => {
      expect(messageApplicationProperties).eql([applicationProperties])
    })
  }).timeout(10000)

  it("declaring a consumer on an existing stream - the consumer should read message annotations", async () => {
    const messageAnnotations: MessageAnnotations[] = []
    const annotations = createAnnotations()
    await publisher.send(Buffer.from("hello"), { messageAnnotations: annotations })

    await client.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) => {
      messageAnnotations.push(message.messageAnnotations || {})
    })

    await eventually(async () => expect(messageAnnotations).eql([annotations]))
  }).timeout(10000)

  it("messageAnnotations with bytes are read correctly", async () => {
    const messageAnnotations: MessageAnnotations[] = []
    const annotations = { test: new AmqpByte(123) }
    await rabbit.createStream("testQ")
    await client.declareConsumer(
      { stream: "testQ", offset: Offset.next(), consumerRef: "test" },
      (message: Message) => {
        messageAnnotations.push(message.messageAnnotations ?? {})
      }
    )

    const testP = await client.declarePublisher({ stream: "testQ" })
    await testP.send(Buffer.from("Hello"), { messageAnnotations: annotations })

    await eventually(async () => {
      const [messageAnnotation] = messageAnnotations
      expect(messageAnnotation).to.eql({ test: 123 })
    })
  }).timeout(10000)

  it("testing if messageHeader and amqpValue is decoded correctly using dataReader", async () => {
    const bufferedInput = readFileSync(path.join(...["test", "data", "header_amqpvalue_message"]))
    const dataReader = new BufferDataReader(bufferedInput)
    const header = createMessageHeader()
    const amqpValue = "amqpValue"

    const message = decodeMessageTesting(dataReader, bufferedInput.length)

    await eventually(async () => {
      expect(message.messageHeader).eql(header)
      expect(message.amqpValue).eql(amqpValue)
    })
  }).timeout(10000)

  it("consumers for the same stream and node should share the underlying connection", async () => {
    const consumersToCreate = getTestNodesFromEnv().length + 1
    const counts = new Map<string, number>()
    for (let i = 0; i < consumersToCreate; i++) {
      const consumer = await createConsumer(streamName, client)
      const { id } = consumer.getConnectionInfo()
      counts.set(id, (counts.get(id) || 0) + 1)
    }

    const countConsumersSharingLocalPort = Array.from(counts.entries()).find(([_id, count]) => count > 1)
    expect(countConsumersSharingLocalPort).not.undefined
  }).timeout(10000)

  it("if a large number of consumers for the same stream is declared, eventually a new client is instantiated even for the same stream/node", async () => {
    const consumersToCreate = (getMaxSharedConnectionInstances() + 1) * (getTestNodesFromEnv().length + 1)
    const counts = new Map<string, number>()
    for (let i = 0; i < consumersToCreate; i++) {
      const consumer = await createConsumer(streamName, client)
      const { id } = consumer.getConnectionInfo()
      counts.set(id, (counts.get(id) || 0) + 1)
    }

    const countConsumersOverLimit = Array.from(counts.entries()).find(
      ([_id, count]) => count > getMaxSharedConnectionInstances()
    )
    expect(countConsumersOverLimit).is.undefined
    expect(Array.from(counts.keys()).length).gt(1)
  }).timeout(10000)

  it("on a new connection, consumerId restarts from 0", async () => {
    const consumersToCreate = (getMaxSharedConnectionInstances() + 1) * (getTestNodesFromEnv().length + 1)
    const consumerIds: number[] = []
    for (let i = 0; i < consumersToCreate; i++) {
      const consumer = await createConsumer(streamName, client)
      consumerIds.push(consumer.consumerId)
    }

    expect(consumerIds.filter((id) => id === 0).length).gt(1)
  }).timeout(10000)

  it("declaring more than 256 consumers should not throw but rather open up multiple connections", async () => {
    const publishersToCreate = 257
    const counts = new Map<string, number>()
    for (let i = 0; i < publishersToCreate; i++) {
      const consumer = await createConsumer(streamName, client)
      const { id } = consumer.getConnectionInfo()
      counts.set(id, (counts.get(id) || 0) + 1)
    }

    expect(Array.from(counts.keys()).length).gt(1)
  }).timeout(10000)

  describe("when the client declares a named connection", () => {
    let connectionName: string | undefined = undefined

    beforeEach(async () => {
      try {
        await client.close()
        connectionName = `consumer-${randomUUID()}`
        client = await createClient(username, password, undefined, undefined, undefined, undefined, connectionName)
      } catch (e) {}
    })
    it("the name is inherited on the consumer connection", async () => {
      await createConsumer(streamName, client)

      await eventually(async () => {
        const connections = await rabbit.getConnections()
        expect(connections.length).eql(2)
        expect(connections).to.satisfy((conns: RabbitConnectionResponse[]) => {
          return conns.every((conn) => conn.client_properties?.connection_name === connectionName)
        })
      }, 5000)
    }).timeout(6000)
  })
})

function createProperties(): MessageProperties {
  return {
    contentType: "contentType",
    contentEncoding: "contentEncoding",
    replyTo: "replyTo",
    to: "to",
    subject: "subject",
    correlationId: "correlationIdAAA",
    messageId: "messageId",
    userId: Buffer.from("userId"),
    absoluteExpiryTime: new Date(),
    creationTime: new Date(),
    groupId: "groupId",
    groupSequence: 666,
    replyToGroupId: "replyToGroupId",
  }
}

function createApplicationProperties(): MessageApplicationProperties {
  return {
    application1: "application1",
    application2: 666,
  }
}

function createAnnotations(): MessageAnnotations {
  return {
    akey1: "value1",
    akey2: "value2",
    akey3: 3,
  }
}

function createMessageHeader(): MessageHeader {
  return {
    deliveryCount: 300,
    durable: true,
    ttl: 0,
    firstAcquirer: true,
    priority: 100,
  }
}
