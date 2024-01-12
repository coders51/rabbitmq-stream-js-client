import { expect } from "chai"
import { Client } from "../../src"
import { Message } from "../../src/producer"
import { range } from "../../src/util"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"

describe("declare super stream consumer", () => {
  let superStreamName: string
  const rabbit = new Rabbit(username, password)
  let client: Client
  let noOfPartitions: number = 0
  const testMessageContent = "test message"

  beforeEach(async () => {
    client = await createClient(username, password)
    superStreamName = createStreamName()
    noOfPartitions = await rabbit.createSuperStream(superStreamName)
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
    // TODO: swap with superstream publisher when it's done -- 11/01/24 -- Luca
    const publisher = await client.declarePublisher({ stream: `${superStreamName}-${Math.floor(Math.random() * 3)}` })
    await publisher.send(Buffer.from(testMessageContent))
    const messages: Message[] = []

    await client.declareSuperStreamConsumer(superStreamName, (message: Message) => {
      messages.push(message)
    })

    await eventually(() => {
      expect(messages).to.have.length(1)
      const [message] = messages
      expect(message.content.toString()).to.be.eq(testMessageContent)
    })
  })

  it("reading multiple messages", async () => {
    const noOfMessages = 20
    // TODO: swap with superstream publisher when it's done -- 11/01/24 -- Luca
    const publisher = await client.declarePublisher({ stream: `${superStreamName}-${Math.floor(Math.random() * 3)}` })
    for (let i = 0; i < noOfMessages; i++) {
      await publisher.send(Buffer.from(`testMessageContent-${i}`))
    }
    const messages: Message[] = []

    await client.declareSuperStreamConsumer(superStreamName, (message: Message) => {
      messages.push(message)
    })

    await eventually(() => {
      expect(messages).to.have.length(noOfMessages)
    })
  })
})
