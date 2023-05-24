import { expect } from "chai"
import { Connection } from "../../src"
import { Message } from "../../src/producer"
import { Offset } from "../../src/requests/subscribe_request"
import { Rabbit } from "../support/rabbit"
import { expectToThrowAsync, password, username } from "../support/util"
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
    try {
      await rabbit.deleteStream(testStreamName)
    } catch (e) {}
  })

  it("the consumer is able to track the offset of the stream through queryOffset method", async () => {
    const messages: Buffer[] = []
    const publisher = await connection.declarePublisher({ stream: testStreamName })
    await publisher.send(Buffer.from("hello"))
    const consumer = await connection.declareConsumer(
      { stream: testStreamName, offset: Offset.first(), consumerRef: "my_consumer" },
      (message: Message) => messages.push(message.content)
    )
    await consumer.storeOffset(1n)

    const offset = await consumer.queryOffset()

    expect(offset).eql(1n)
  }).timeout(10000)

  it("declaring a consumer without consumerRef and querying for the offset should rise an error", async () => {
    const consumer = await connection.declareConsumer(
      { stream: testStreamName, offset: Offset.first() },
      (message: Message) => {
        console.log(message.content)
      }
    )
    await expectToThrowAsync(
      () => consumer.queryOffset(),
      Error,
      "ConsumerReference must be defined in order to use this!"
    )
  })

  it("query offset is able to raise an error if the stream is closed", async () => {
    const consumer = await connection.declareConsumer(
      { stream: testStreamName, offset: Offset.first(), consumerRef: "my_consumer" },
      (message: Message) => {
        console.log(message.content)
      }
    )
    await rabbit.deleteStream(testStreamName)
    await expectToThrowAsync(() => consumer.queryOffset(), Error, `Query offset command returned error with code 2`)
  })
})
