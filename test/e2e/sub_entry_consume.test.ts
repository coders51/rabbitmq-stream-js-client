import { expect } from "chai"
import { Connection } from "../../src"
import { Consumer } from "../../src/consumer"
import { Message, Producer } from "../../src/producer"
import { Offset } from "../../src/requests/subscribe_request"
import { createConnection, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"
import { range } from "../../src/util"

describe("consume a batch of messages", () => {
  const rabbit = new Rabbit(username, password)
  let connection: Connection
  let streamName: string
  let publisher: Producer
  let consumer: Consumer | undefined

  beforeEach(async () => {
    connection = await createConnection(username, password)
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
      await consumer?.close()
    } catch (e) {}
  })

  it("consuming a batch of messages without compression should not raise error", async () => {
    const messages = [
      { content: Buffer.from("Ciao") },
      { content: Buffer.from("Ciao1") },
      { content: Buffer.from("Ciao2") },
      { content: Buffer.from("Ciao3") },
      { content: Buffer.from("Ciao4") },
    ]

    await publisher.sendSubEntries(messages)
  }).timeout(10000)

  it("consume a batch of messages - receive the same number of messages", async () => {
    const receivedMessages = []
    consumer = await connection.declareConsumer({ stream: streamName, offset: Offset.first() }, (m: Message) =>
      receivedMessages.push(m)
    )
    const messages = [
      { content: Buffer.from("Ciao") },
      { content: Buffer.from("Ciao1") },
      { content: Buffer.from("Ciao2") },
      { content: Buffer.from("Ciao3") },
      { content: Buffer.from("Ciao4") },
    ]

    await publisher.sendSubEntries(messages)

    await eventually(async () => {
      expect(receivedMessages.length).eql(messages.length)
    }, 10000)
  }).timeout(10000)

  it("consume a batch of messages - each received message contains the one that was sent", async () => {
    const receivedMessages: Message[] = []
    consumer = await connection.declareConsumer({ stream: streamName, offset: Offset.first() }, (m: Message) =>
      receivedMessages.push(m)
    )
    const messageContents = range(5).map((_, i) => `Ciao${i}`)
    const messages = messageContents.map((m) => ({ content: Buffer.from(m) }))

    await publisher.sendSubEntries(messages)

    await eventually(async () => {
      const receivedContent = receivedMessages.map((rm) => rm.content.toString("utf-8"))
      expect(messageContents).to.eql(receivedContent)
    }, 10000)
  }).timeout(10000)
})
