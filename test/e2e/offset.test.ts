import { expect } from "chai"
import { Client } from "../../src"
import { Message } from "../../src/publisher"
import { Offset } from "../../src/requests/subscribe_request"
import { createClient } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import {
  always,
  eventually,
  expectToThrowAsync,
  password,
  sendANumberOfRandomMessages,
  username,
  wait,
} from "../support/util"

describe("offset", () => {
  const rabbit = new Rabbit(username, password)
  const testStreamName = "test-stream"
  let client: Client

  beforeEach(async () => {
    await rabbit.createStream(testStreamName)
    client = await createClient(username, password)
  })

  afterEach(async () => {
    await client.close()
    try {
      await rabbit.deleteStream(testStreamName)
    } catch (e) {}
  })

  describe("reading", () => {
    it("if offset is first, all messages should be read", async () => {
      const receivedMessages: Message[] = []
      const publisher = await client.declarePublisher({ stream: testStreamName })
      const messages = await sendANumberOfRandomMessages(publisher)

      await client.declareConsumer(
        { stream: testStreamName, consumerRef: "my consumer", offset: Offset.first() },
        async (message: Message) => {
          receivedMessages.push(message)
        }
      )

      await eventually(async () => {
        expect(receivedMessages).to.have.length(messages.length)
      })
    })

    it("if offset is next, only the messages sent after the subscription should be read", async () => {
      const receivedMessages: Message[] = []
      const publisher = await client.declarePublisher({ stream: testStreamName })
      await sendANumberOfRandomMessages(publisher)
      const nextMessage = "next message"

      await client.declareConsumer(
        { stream: testStreamName, consumerRef: "my consumer", offset: Offset.next() },
        async (message: Message) => {
          receivedMessages.push(message)
        }
      )
      await publisher.send(Buffer.from(nextMessage))

      await eventually(async () => {
        expect(receivedMessages).to.have.length(1)
        const [receivedMessage] = receivedMessages
        expect(receivedMessage.content.toString()).to.eql(nextMessage)
      })
    })

    it("if offset is last, only the messages sent in the last batch and all subsequent ones should be read", async () => {
      const receivedMessages: Message[] = []
      const publisher = await client.declarePublisher({ stream: testStreamName })
      const previousMessages = await sendANumberOfRandomMessages(publisher)
      await wait(500)
      const lastBatchMessages = await sendANumberOfRandomMessages(publisher, previousMessages.length)

      await client.declareConsumer(
        { stream: testStreamName, consumerRef: "my consumer", offset: Offset.last() },
        async (message: Message) => {
          receivedMessages.push(message)
        }
      )

      await eventually(async () => {
        expect(receivedMessages).to.have.length(lastBatchMessages.length)
      })
    })

    it("if offset is of type numeric and value 0, all messages should be read", async () => {
      const receivedMessages: Message[] = []
      const publisher = await client.declarePublisher({ stream: testStreamName })
      const messages = await sendANumberOfRandomMessages(publisher)

      await client.declareConsumer(
        { stream: testStreamName, consumerRef: "my consumer", offset: Offset.offset(0n) },
        async (message: Message) => {
          receivedMessages.push(message)
        }
      )

      await eventually(async () => {
        expect(receivedMessages).to.have.length(messages.length)
      })
    })

    it("if offset is of type numeric, only the messages with offset higher or equal to the requested offset should be read", async () => {
      const receivedMessages: Message[] = []
      const publisher = await client.declarePublisher({ stream: testStreamName })
      const messages = await sendANumberOfRandomMessages(publisher)
      const offset = Math.floor(Math.random() * messages.length)

      await client.declareConsumer(
        {
          stream: testStreamName,
          consumerRef: "my consumer",
          offset: Offset.offset(BigInt(offset)),
        },
        async (message: Message) => {
          receivedMessages.push(message)
        }
      )

      await eventually(async () => {
        expect(receivedMessages).to.have.length(messages.length - offset)
      })
    })

    it("if offset is of type numeric and value greater than the number of messages, no messages should be read", async () => {
      const receivedMessages: Message[] = []
      const publisher = await client.declarePublisher({ stream: testStreamName })
      const messages = await sendANumberOfRandomMessages(publisher)
      const offset = messages.length + 1

      await client.declareConsumer(
        { stream: testStreamName, consumerRef: "my consumer", offset: Offset.offset(BigInt(offset)) },
        async (message: Message) => {
          receivedMessages.push(message)
        }
      )

      await always(async () => {
        expect(receivedMessages).to.have.length(0)
      }, 5000)
    }).timeout(10000)

    it("if offset is of type timestamp, all the messages belonging to batches sent earlier than the timestamp should be skipped", async () => {
      const receivedMessages: Message[] = []
      const publisher = await client.declarePublisher({ stream: testStreamName })
      const previousMessages = await sendANumberOfRandomMessages(publisher)
      await wait(10)
      const offset = new Date()
      const laterMessages = await sendANumberOfRandomMessages(publisher, previousMessages.length)

      await client.declareConsumer(
        { stream: testStreamName, consumerRef: "my consumer", offset: Offset.timestamp(offset) },
        async (message: Message) => {
          receivedMessages.push(message)
        }
      )

      await eventually(async () => {
        expect(receivedMessages).to.have.length(laterMessages.length)
      })
    })
  })

  describe("store", () => {
    it("saving the store offset of a stream correctly", async () => {
      let offset: bigint = 0n
      const consumer = await client.declareConsumer(
        { stream: testStreamName, consumerRef: "my consumer", offset: Offset.next() },
        async (message: Message) => {
          await consumer.storeOffset(message.offset!)
          offset = message.offset!
        }
      )
      const publisher = await client.declarePublisher({ stream: testStreamName })

      await publisher.send(Buffer.from("hello"))
      await publisher.send(Buffer.from("world"))

      await eventually(async () => {
        const result = await consumer.queryOffset()
        expect(result).eql(offset)
      })
    }).timeout(10000)

    it("declaring a consumer without consumerRef and saving the store offset should rise an error", async () => {
      const consumer = await client.declareConsumer(
        { stream: testStreamName, offset: Offset.first() },
        (_message: Message) => {
          return
        }
      )
      await expectToThrowAsync(
        () => consumer.storeOffset(1n),
        Error,
        "ConsumerReference must be defined in order to use this!"
      )
    })
  })

  describe("query", () => {
    it("the consumer is able to track the offset and start from the stored offset", async () => {
      const consumerOneMessages: Message[] = []
      const consumerTwoMessages: Message[] = []
      const publisher = await client.declarePublisher({ stream: testStreamName })
      await publisher.send(Buffer.from("hello"))
      await publisher.send(Buffer.from("marker"))
      await publisher.send(Buffer.from("hello"))
      await publisher.send(Buffer.from("world"))

      const consumer = await client.declareConsumer(
        { stream: testStreamName, offset: Offset.first(), consumerRef: "my_consumer" },
        async (message: Message) => {
          consumerOneMessages.push(message)
          if (message.content.toString() === "marker") {
            await consumer.storeOffset(message.offset!)
          }
        }
      )
      await eventually(async () => {
        const [foundMarker] = consumerOneMessages.filter((msg) => msg.content.toString() === "marker")
        expect(foundMarker).to.not.be.undefined
        const offset = await client.queryOffset({ stream: testStreamName, reference: "my_consumer" })
        expect(offset).to.not.be.undefined
      }, 3000)
      const storedOffset = await client.queryOffset({ stream: testStreamName, reference: "my_consumer" })
      await client.declareConsumer(
        { stream: testStreamName, offset: Offset.offset(storedOffset), consumerRef: "my_consumer" },
        async (message: Message) => {
          consumerTwoMessages.push(message)
        }
      )

      await eventually(async () => {
        expect(consumerTwoMessages.length).eql(3)
      })
    }).timeout(10000)

    it("declaring a consumer without consumerRef and querying for the offset should rise an error", async () => {
      const consumer = await client.declareConsumer(
        { stream: testStreamName, offset: Offset.first() },
        (_message: Message) => {
          return
        }
      )
      await expectToThrowAsync(
        () => consumer.queryOffset(),
        Error,
        "ConsumerReference must be defined in order to use this!"
      )
    })

    it("query offset is able to raise an error if the stream is closed", async () => {
      const consumer = await client.declareConsumer(
        { stream: testStreamName, offset: Offset.first(), consumerRef: "my_consumer" },
        (_message: Message) => {
          return
        }
      )
      await rabbit.deleteStream(testStreamName)
      await wait(200)
      await expectToThrowAsync(() => consumer.queryOffset(), Error, `This socket has been ended by the other party`)
    })
  })
})
