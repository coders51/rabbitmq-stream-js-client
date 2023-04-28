import { expect } from "chai"
import { Connection } from "../../src"
import { Message, Producer } from "../../src/producer"
import { Offset } from "../../src/requests/subscribe_request"
import { createConnection, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync, range } from "../support/util"

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

  it("should handle the message", async () => {
    const messages: Buffer[] = []
    await connection.declareConsumer({ stream: streamName, offset: Offset.next() }, (message: Message) =>
      messages.push(message.content)
    )

    await publisher.send(Buffer.from("hello"))

    await eventually(() => expect(messages).eql([Buffer.from("hello")]))
  }).timeout(10000)

  it("declaring a consumer on an existing stream - the consumer should be handle more then one message", async () => {
    const messages: Buffer[] = []
    await connection.declareConsumer({ stream: streamName, offset: Offset.next() }, (message: Message) =>
      messages.push(message.content)
    )

    await publisher.send(Buffer.from("hello"))
    await publisher.send(Buffer.from("world"))

    await eventually(() => expect(messages).eql([Buffer.from("hello"), Buffer.from("world")]))
  }).timeout(10000)

  it(`consume a lot of messages`, async () => {
    const receivedMessages: Buffer[] = []
    //const receivedClassicMessages: Buffer[] = []

    await connection.declareConsumer({ stream: streamName, offset: Offset.next() }, (message: Message) => {
      receivedMessages.push(message.content)
    })

    const messages = range(1000).map((n) => Buffer.from(`hello${n}`))
    for (const m of messages) {
      await publisher.send(m)
    }

    // const { conn, ch } = await createClassicConsumer(streamName, (m) => receivedClassicMessages.push(m.content))
    // await eventually(() => expect(receivedClassicMessages).eql(messages), 7000)
    // await ch.close()
    // await conn.close()
    await eventually(() => expect(receivedMessages).eql(messages), 7000)
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
})
