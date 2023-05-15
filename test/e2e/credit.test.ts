import { expect } from "chai"
import { Connection } from "../../src"
import { Message } from "../../src/producer"
import { Offset } from "../../src/requests/subscribe_request"
import { createConnection, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"

describe.skip("credit management", () => {
  const rabbit = new Rabbit(username, password)
  let streamName: string
  let connection: Connection

  beforeEach(async () => {
    connection = await createConnection(username, password)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    try {
      await connection.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  // This test can only run locally, the HTTP API gives different results in GitHub CI (https://coders51.slack.com/archives/C03E263HH38/p1681374600592829)
  it(`the number of credit remain stable after have consumed some messages`, async () => {
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
      const allConsumerCredits = await rabbit.returnConsumersCredits()
      expect(allConsumerCredits[0].allCredits[0]).eql(10)
    }, 5000)
  }).timeout(20000)
})
