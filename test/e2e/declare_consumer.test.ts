import { expect } from "chai"
import { Connection } from "../../src"
import {
  Message,
  MessageAnnotations,
  MessageApplicationProperties,
  MessageProperties,
  Producer,
  MessageHeader,
} from "../../src/producer"
import { Offset } from "../../src/requests/subscribe_request"
import { createConnection, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { range } from "../../src/util"
import { BufferDataReader } from "../../src/response_decoder"
import {
  eventually,
  expectToThrowAsync,
  username,
  password,
  createClassicPublisher,
  decodeMessageTesting,
} from "../support/util"
import { readFileSync } from "fs"
import path from "path"

describe("declare consumer", () => {
  let streamName: string
  let nonExistingStreamName: string
  const rabbit = new Rabbit(username, password)
  let connection: Connection
  let publisher: Producer

  beforeEach(async () => {
    connection = await createConnection(username, password)
    streamName = createStreamName()
    nonExistingStreamName = createStreamName()
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

  it("declaring a consumer on an existing stream - the consumer should handle the message", async () => {
    const messages: Buffer[] = []
    await publisher.send(Buffer.from("hello"))

    await connection.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) =>
      messages.push(message.content),
    )

    await eventually(() => expect(messages).eql([Buffer.from("hello")]))
  }).timeout(10000)

  it("declaring a consumer on an existing stream - the consumer should handle more then one message", async () => {
    const messages: Buffer[] = []
    await publisher.send(Buffer.from("hello"))
    await publisher.send(Buffer.from("world"))
    await publisher.send(Buffer.from("world"))

    await connection.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) =>
      messages.push(message.content),
    )

    await eventually(() => expect(messages).eql([Buffer.from("hello"), Buffer.from("world"), Buffer.from("world")]))
  }).timeout(10000)

  it.skip(`consume a lot of messages`, async () => {
    const receivedMessages: Buffer[] = []
    await connection.declareConsumer({ stream: streamName, offset: Offset.next() }, (message: Message) => {
      receivedMessages.push(message.content)
    })

    const messages = range(1000).map((n) => Buffer.from(`hello${n}`))
    for (const m of messages) {
      await publisher.send(m)
    }

    await eventually(() => expect(receivedMessages).eql(messages), 10000)
  }).timeout(50000)

  it("declaring a consumer on a non-existing stream should raise an error", async () => {
    await expectToThrowAsync(
      () =>
        connection.declareConsumer({ stream: nonExistingStreamName, offset: Offset.first() }, (message: Message) =>
          console.log(message.content),
        ),
      Error,
      "Declare Consumer command returned error with code 2 - Stream does not exist",
    )
  })

  it("declaring a consumer on an existing stream - the consumer should read message properties", async () => {
    const messageProperties: MessageProperties[] = []
    const properties = createProperties()
    await publisher.send(Buffer.from("hello"), { messageProperties: properties })

    await connection.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) => {
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

    await connection.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) => {
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

    await connection.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) => {
      messageAnnotations.push(message.messageAnnotations || {})
    })

    await eventually(async () => expect(messageAnnotations).eql([annotations]))
  }).timeout(10000)

  it("messageAnnotations are ignored by a classic driver", async () => {
    const messageAnnotations: MessageAnnotations[] = []
    const annotations = createAnnotations()
    const classicPublisher = await createClassicPublisher()
    await classicPublisher.ch.assertQueue("testQ", {
      exclusive: false,
      durable: true,
      autoDelete: false,
      arguments: {
        "x-queue-type": "stream", // Mandatory to define stream queue
      },
    })
    classicPublisher.ch.sendToQueue("testQ", Buffer.from("Hello"), {
      headers: {
        messageAnnotations: annotations,
      },
    })

    await connection.declareConsumer({ stream: "testQ", offset: Offset.first() }, (message: Message) => {
      messageAnnotations.push(message.messageAnnotations || {})
    })

    await eventually(async () => {
      expect(messageAnnotations).not.eql([annotations])
      await classicPublisher.ch.close()
      await classicPublisher.conn.close()
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
