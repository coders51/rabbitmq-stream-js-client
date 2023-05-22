import { expect } from "chai"
import { Connection } from "../../src"
import { Message } from "../../src/producer"
import { Offset } from "../../src/requests/subscribe_request"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync, password, username } from "../support/util"
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

  it.skip("saving the store offset of a stream correctly", async () => {
    const messages: Buffer[] = []
    const publisher = await connection.declarePublisher({ stream: testStreamName })
    await publisher.send(Buffer.from("hello"))
    const consumer = await connection.declareConsumer(
      { stream: testStreamName, consumerRef: "my consumer", offset: Offset.first() },
      (message: Message) => messages.push(message.content)
    )

    await consumer.storeOffset(Offset.offset(1n).value!)

    // TODO - change expect, similar to the credit one
    await eventually(() => expect(messages).eql([Buffer.from("hello")]))
  }).timeout(10000)

  it("declaring a consumer without consumerRef and saving the store offset should rise an error", async () => {
    const consumer = await connection.declareConsumer(
      { stream: testStreamName, offset: Offset.first() },
      (message: Message) => {
        console.log(message.content)
      }
    )
    await expectToThrowAsync(
      () => consumer.storeOffset(1n),
      Error,
      "ConsumerReference must be defined in order to use this!"
    )
  })
})
