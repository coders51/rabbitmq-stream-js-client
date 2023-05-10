import { expect } from "chai"
import { Connection } from "../../src"
import { Message, MessageApplicationProperties, MessageProperties, Producer } from "../../src/producer"
import { Offset } from "../../src/requests/subscribe_request"
import { createConnection, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync } from "../support/util"
import { range } from "../../src/util"

describe("declare consumer", () => {
  const rabbit = new Rabbit()
  let streamName: string
  let nonExistingStreamName: string
  let connection: Connection
  let publisher: Producer

  beforeEach(async () => {
    connection = await createConnection()
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
      messages.push(message.content)
    )

    await eventually(() => expect(messages).eql([Buffer.from("hello")]))
  }).timeout(10000)

  it("declaring a consumer on an existing stream - the consumer should handle more then one message", async () => {
    const messages: Buffer[] = []
    await publisher.send(Buffer.from("hello"))
    await publisher.send(Buffer.from("world"))
    await publisher.send(Buffer.from("world"))

    await connection.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) =>
      messages.push(message.content)
    )

    await eventually(() => expect(messages).eql([Buffer.from("hello"), Buffer.from("world"), Buffer.from("world")]))
  }).timeout(10000)

  it(`consume a lot of messages`, async () => {
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
          console.log(message.content)
        ),
      Error,
      "Declare Consumer command returned error with code 2 - Stream does not exist"
    )
  })

  it("declaring a consumer on an existing stream - the consumer should read message properties", async () => {
    const messageProperties: MessageProperties[] = []
    const messages: string[] = []
    const properties = createProperties()
    await publisher.send(Buffer.from("hello"), { messageProperties: properties })

    await connection.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) => {
      messageProperties.push(message.messageProperties || {})
      messages.push("JSON.stringify(message.properties?.correlationId) ||")
    })

    await eventually(async () => {
      expect(messageProperties).eql([properties])
    })
  }).timeout(10000)

  it("declaring a consumer on an existing stream - the consumer should read application properties", async () => {
    const messageApplicationProperties: MessageApplicationProperties[] = []
    const messages: string[] = []
    const applicationProperties = createApplicationProperties()
    await publisher.send(Buffer.from("hello"), { applicationProperties })

    await connection.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) => {
      messageApplicationProperties.push(message.applicationProperties || {})
      messages.push("JSON.stringify(message.properties?.correlationId) ||")
    })

    await eventually(async () => {
      expect(messageApplicationProperties).eql([applicationProperties])
    })
  }).timeout(10000)
})

function createProperties(): MessageProperties {
  return {
    contentType: `contentType`,
    contentEncoding: `contentEncoding`,
    replyTo: `replyTo`,
    to: `to`,
    subject: `subject`,
    correlationId: `correlationIdAAA`,
    messageId: `messageId`,
    userId: Buffer.from(`userId`),
    absoluteExpiryTime: new Date(),
    creationTime: new Date(),
    groupId: `groupId`,
    groupSequence: 666,
    replyToGroupId: `replyToGroupId`,
  }
}

function createApplicationProperties(): MessageApplicationProperties {
  return {
    application1: "application1",
    application2: 666,
  }
}
