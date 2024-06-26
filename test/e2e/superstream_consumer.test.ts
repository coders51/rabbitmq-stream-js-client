import { expect } from "chai"
import { Client, Offset } from "../../src"
import { Message } from "../../src/publisher"
import { range } from "../../src/util"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"
import { randomUUID } from "crypto"

describe("super stream consumer", () => {
  let superStreamName: string
  const rabbit = new Rabbit(username, password)
  let client: Client
  let noOfPartitions: number = 0
  let sender: (noOfMessages: number) => Promise<void>

  beforeEach(async () => {
    client = await createClient(username, password)
    superStreamName = createStreamName()
    noOfPartitions = await rabbit.createSuperStream(superStreamName)
    sender = await messageSender(client, superStreamName)
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
    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (_message: Message) => {
      return
    })
  })

  it("declaring a super stream consumer on an existing super stream - read a message", async () => {
    await sender(1)
    const messages: Message[] = []

    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (message: Message) => {
      messages.push(message)
    })

    await eventually(() => {
      expect(messages).to.have.length(1)
      const [message] = messages
      expect(message.content.toString()).to.be.eq(`${testMessageContent}-0`)
    })
  })

  it("for a consumer the number of connections should be equals to the partitions' number", async () => {
    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (_) => {
      return
    })

    await eventually(() => {
      expect(client.consumerCounts()).to.be.eql(noOfPartitions)
    })
  })

  it("reading multiple messages - each message should be read only once", async () => {
    const noOfMessages = 20
    await sender(noOfMessages)
    const messages: Message[] = []

    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (message: Message) => {
      messages.push(message)
    })

    await eventually(() => {
      expect(messages).to.have.length(noOfMessages)
    })
  })

  it("multiple composite consumers with same consumerRef - each message should be read only once", async () => {
    const noOfMessages = 20
    const messages: Message[] = []

    await client.declareSuperStreamConsumer(
      { superStream: superStreamName, consumerRef: "counting-messages" },
      (message: Message) => messages.push(message)
    )
    await client.declareSuperStreamConsumer(
      { superStream: superStreamName, consumerRef: "counting-messages" },
      (message: Message) => messages.push(message)
    )

    await sender(noOfMessages)

    await eventually(() => {
      expect(messages).to.have.length(noOfMessages)
    })
  })

  it("reading multiple messages - get messages only at a specific consuming point timestamp", async () => {
    const noOfMessages = 20
    await sender(5)
    const sleepingTime = 5000
    await sleep(sleepingTime)
    await sender(noOfMessages)
    const messages: Message[] = []

    await client.declareSuperStreamConsumer(
      {
        superStream: superStreamName,
        offset: Offset.timestamp(new Date(Date.now() - (sleepingTime - 1000))),
      },
      (message: Message) => {
        messages.push(message)
      }
    )

    await eventually(() => {
      expect(messages).to.have.length(noOfMessages)
    })
  }).timeout(10000)

  it("closing the locator closes all connections", async () => {
    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (_) => {
      return
    })

    await client.close()

    await eventually(async () => {
      const connections = await rabbit.getConnections()
      expect(connections).to.have.length(0)
    }, 5000)
  }).timeout(5000)
})

const testMessageContent = "test message"

const messageSender = async (client: Client, superStreamName: string) => {
  const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName }, () => randomUUID())

  const sendMessages = async (noOfMessages: number) => {
    for (let i = 0; i < noOfMessages; i++) {
      await publisher.send(Buffer.from(`${testMessageContent}-${i}`), {})
    }
  }

  return sendMessages
}

const sleep = (ms: number) => {
  return new Promise((res) => {
    setTimeout(() => {
      res(true)
    }, ms)
  })
}
