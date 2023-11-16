import { expect } from "chai"
import { Connection } from "../../src"
import { Message } from "../../src/producer"
import { Offset } from "../../src/requests/subscribe_request"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync, password, username } from "../support/util"
import { createConnection } from "../support/fake_data"

describe("offset", () => {
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

  describe("store", () => {
    it("saving the store offset of a stream correctly", async () => {
      let offset: bigint = 0n
      const consumer = await connection.declareConsumer(
        { stream: testStreamName, consumerRef: "my consumer", offset: Offset.next() },
        async (message: Message) => {
          await consumer.storeOffset(message.offset!)
          offset = message.offset!
        },
      )
      const publisher = await connection.declarePublisher({ stream: testStreamName })

      await publisher.send(Buffer.from("hello"))
      await publisher.send(Buffer.from("world"))

      await eventually(async () => {
        const result = await consumer.queryOffset()
        expect(result).eql(offset)
      })
    }).timeout(10000)

    it("declaring a consumer without consumerRef and saving the store offset should rise an error", async () => {
      const consumer = await connection.declareConsumer(
        { stream: testStreamName, offset: Offset.first() },
        (_message: Message) => {
          return
        },
      )
      await expectToThrowAsync(
        () => consumer.storeOffset(1n),
        Error,
        "ConsumerReference must be defined in order to use this!",
      )
    })
  })

  describe("query", () => {
    it("the consumer is able to track the offset of the stream through queryOffset method", async () => {
      let offset: bigint = 0n
      const consumer = await connection.declareConsumer(
        { stream: testStreamName, offset: Offset.next(), consumerRef: "my_consumer" },
        async (message: Message) => {
          await consumer.storeOffset(message.offset!)
          offset = message.offset!
        },
      )
      const publisher = await connection.declarePublisher({ stream: testStreamName })

      await publisher.send(Buffer.from("hello"))
      await publisher.send(Buffer.from("world"))

      await eventually(async () => {
        const result = await consumer.queryOffset()
        expect(result).eql(offset)
      })
    }).timeout(10000)

    it("declaring a consumer without consumerRef and querying for the offset should rise an error", async () => {
      const consumer = await connection.declareConsumer(
        { stream: testStreamName, offset: Offset.first() },
        (_message: Message) => {
          return
        },
      )
      await expectToThrowAsync(
        () => consumer.queryOffset(),
        Error,
        "ConsumerReference must be defined in order to use this!",
      )
    })

    it("query offset is able to raise an error if the stream is closed", async () => {
      const consumer = await connection.declareConsumer(
        { stream: testStreamName, offset: Offset.first(), consumerRef: "my_consumer" },
        (_message: Message) => {
          return
        },
      )
      await rabbit.deleteStream(testStreamName)
      await expectToThrowAsync(() => consumer.queryOffset(), Error, `Query offset command returned error with code 2`)
    })
  })
})
