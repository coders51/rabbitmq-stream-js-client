import { expect } from "chai"
import { Client } from "../../src"
import { Message } from "../../src/publisher"
import { range } from "../../src/util"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"

describe("declare super stream consumer", () => {
  let superStreamName: string
  const rabbit = new Rabbit(username, password)
  let client: Client
  let noOfPartitions: number = 0
  let sender: (noOfMessages: number) => Promise<void>

  beforeEach(async () => {
    client = await createClient(username, password)
    superStreamName = createStreamName()
    noOfPartitions = await rabbit.createSuperStream(superStreamName)
    sender = await messageSender(client, superStreamName, noOfPartitions)
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteSuperStream(superStreamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("querying partitions - return the same number of partitions", async () => {
    const partitions = await client.queryPartitions({ superStream: superStreamName })

    expect(partitions.length).to.be.equal(noOfPartitions)
  })

  it("querying partitions - return the name of the streams making up the superstream", async () => {
    const partitions = await client.queryPartitions({ superStream: superStreamName })

    expect(range(noOfPartitions).map((i) => `${superStreamName}-${i}`)).to.deep.eq(partitions)
  })

  it("declaring a super stream consumer on an existing super stream - no error is thrown", async () => {
    await client.declareSuperStreamConsumer(superStreamName, (_message: Message) => {
      return
    })
  })

  it("declaring a super stream consumer on an existing super stream - read a message", async () => {
    await sender(1)
    const messages: Message[] = []

    await client.declareSuperStreamConsumer(superStreamName, (message: Message) => {
      messages.push(message)
    })

    await eventually(() => {
      expect(messages).to.have.length(1)
      const [message] = messages
      expect(message.content.toString()).to.be.eq(`${testMessageContent}-0`)
    })
  })

  it("reading multiple messages - each message should be read only once", async () => {
    const noOfMessages = 20
    await sender(noOfMessages)
    const messages: Message[] = []

    await client.declareSuperStreamConsumer(superStreamName, (message: Message) => {
      messages.push(message)
    })

    await eventually(() => {
      expect(messages).to.have.length(noOfMessages)
    })
  })

  it("reading multiple messages - the order should be preserved", async () => {
    const noOfMessages = 20
    await sender(noOfMessages)
    const messages: Message[] = []

    await client.declareSuperStreamConsumer(superStreamName, (message: Message) => {
      messages.push(message)
    })

    await eventually(() => {
      expect(range(noOfMessages).map((i) => `${testMessageContent}-${i}`)).to.deep.equal(
        messages.map((m) => m.content.toString()),
      )
    })
  })
})

const testMessageContent = "test message"

const messageSender = async (client: Client, superStreamName: string, noOfPartitions: number) => {
  // TODO: swap with superstream publisher when it's done -- 11/01/24 -- LM
  const publisher = await client.declarePublisher({
    stream: `${superStreamName}-${Math.floor(Math.random() * noOfPartitions)}`,
  })

  const sendMessages = async (noOfMessages: number) => {
    for (let i = 0; i < noOfMessages; i++) {
      await publisher.send(Buffer.from(`${testMessageContent}-${i}`))
    }
  }

  return sendMessages
}
