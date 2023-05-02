import { expect } from "chai"
import { Connection, connect } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"
import { Offset } from "../../src/requests/subscribe_request"
import { Message } from "../../src/producer"

describe("credit management", () => {
  const rabbit = new Rabbit()
  const streamName = "credit-test-stream"
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
        credit: (_data) => console.info("Subscribe server error"),
      },
    })
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    await connection.close()
    await rabbit.deleteStream(streamName)
  })

  // This test can only run locally, the HTTP API gives different results in GitHub CI (https://coders51.slack.com/archives/C03E263HH38/p1681374600592829)
  it.skip(`the number of credit remain stable after have consumed some messages`, async () => {
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
      expect(receivedMessages).eql(messages)
      const credits = await rabbit.returnSingleConsumerCredits()
      expect(credits).eql(10)
    }, 5000)
  }).timeout(20000)
})
