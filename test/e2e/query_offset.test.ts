import { expect } from "chai"
import { Connection, connect } from "../../src"
import { Message } from "../../src/producer"
import { Offset } from "../../src/requests/subscribe_request"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"

describe("declare consumer", () => {
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

  it("the consumer is able to track the offset of the stream through queryOffset method", async () => {
    const messages: Buffer[] = []
    const publisher = await connection.declarePublisher({ stream: testStreamName })
    await publisher.send(Buffer.from("hello"))
    const consumer = await connection.declareConsumer(
      { stream: testStreamName, offset: Offset.first(), consumerRef: "my consumer" },
      (message: Message) => messages.push(message.content)
    )
    await consumer.storeOffset(1n)

    const offset = await consumer.queryOffset()

    await eventually(() => expect(offset).eql(1n))
  }).timeout(10000)
})
