import { expect } from "chai"
import { Connection, connect } from "../../src"
import { Message } from "../../src/producer"
import { Offset } from "../../src/requests/subscribe_request"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"

describe("close consumer", () => {
  const rabbit = new Rabbit()
  const testStreamName = "test-stream"
  let connection: Connection

  beforeEach(async () => {
    await rabbit.createStream(testStreamName)

    connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })
  })

  afterEach(async () => {
    await connection.close()
    await rabbit.deleteStream(testStreamName)
  })

  it("closing a consumer in an existing stream", async () => {
    const messages: Buffer[] = []
    await connection.declarePublisher({ stream: testStreamName })
    const consumer = await connection.declareConsumer(
      { stream: testStreamName, offset: Offset.first() },
      (message: Message) => messages.push(message.content)
    )

    const response = await connection.closeConsumer(consumer.consumerId)

    await eventually(() => expect(response).eql(true))
  }).timeout(10000)
})
