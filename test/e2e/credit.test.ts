import { expect } from "chai"
import { Connection, connect } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"
import { Offset } from "../../src/requests/subscribe_request"
import { Message } from "../../src/producer"

describe("update the metadata from the server", () => {
  const rabbit = new Rabbit()
  const streamName = "test-stream"
  let connection: Connection

  beforeEach(async () => {
    connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0, // not used
      heartbeat: 0, // not used
      listeners: {
        metadata_update: (_data) => console.info("Subscribe server error"),
      },
    })
  })
  beforeEach(async () => {
    try {
      await rabbit.deleteStream(streamName)
    } catch (error) {}
  })
  beforeEach(() => rabbit.createStream(streamName))

  afterEach(() => connection.close())

  it(`after declaring a consumer with initialCredit 10, and consuming 2 messages 
    the consumer should still have 10 credits`, async () => {
    const receivedMessages: Buffer[] = []
    const howMany = 2
    const messages = Array.from(Array(howMany).keys()).map((_) => Buffer.from("hello"))
    const publisher = await connection.declarePublisher({ stream: streamName })
    for (const m of messages) {
      await publisher.send(m)
    }

    await connection.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) =>
      receivedMessages.push(message.content)
    )

    await eventually(async () => {
      const allConsumerCredits = await rabbit.returnConsumersCredits()
      expect(allConsumerCredits[0].allCredits[0]).eql(10)
    })
    await eventually(() => expect(receivedMessages).eql(messages))
  }).timeout(10000)
})
