import { expect } from "chai"
import { Connection } from "../../src"
import { Message } from "../../src/producer"
import { Offset } from "../../src/requests/subscribe_request"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"
import { createConnection } from "../support/fake_data"

describe("declare consumer", () => {
  const rabbit = new Rabbit(username, password)
  const testStreamName = "test-stream"
  let connection: Connection

  beforeEach(async () => {
    await rabbit.createStream(testStreamName)
    connection = await createConnection(username, password)
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
