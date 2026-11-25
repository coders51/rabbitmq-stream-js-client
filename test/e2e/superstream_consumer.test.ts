import { expect } from "chai"
import { Client, Offset } from "../../src"
import { Consumer } from "../../src/consumer"
import { Message, MessageOptions } from "../../src/publisher"
import { range } from "../../src/util"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username, waitSleeping } from "../support/util"
import { randomUUID } from "crypto"
import { creditsOnChunkCompleted } from "../../src/consumer_credit_policy"

describe("super stream consumer", () => {
  let superStreamName: string
  const rabbit = new Rabbit(username, password)
  let client: Client
  let noOfPartitions: number = 0
  let sender: (noOfMessages: number) => Promise<void>

  beforeEach(async () => {
    client = await createClient(username, password)
    superStreamName = createStreamName()
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteSuperStream(superStreamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  describe("random partitioning", () => {
    beforeEach(async () => {
      noOfPartitions = await rabbit.createSuperStream(superStreamName)
      sender = await messageSender(client, superStreamName)
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

    it("declaring an async super stream consumer on an existing super stream - no error is thrown", async () => {
      await client.declareSuperStreamConsumer({ superStream: superStreamName }, async (_message: Message) => {
        await waitSleeping(10)
        return
      })
    })

    it("declaring a super stream consumer with a custom credit policy - no error is thrown", async () => {
      await client.declareSuperStreamConsumer(
        { superStream: superStreamName, creditPolicy: creditsOnChunkCompleted(2, 1) },
        (_message: Message) => {
          return
        }
      )
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

    it("closing a super stream consumer - un-registers all partition consumers from the client", async () => {
      const superStreamConsumer = await client.declareSuperStreamConsumer({ superStream: superStreamName }, (_) => {
        return
      })
      expect(client.consumerCounts()).to.be.equal(noOfPartitions)

      await superStreamConsumer.close()

      expect(client.consumerCounts()).to.be.equal(0)
    })

    it("closing a super stream consumer twice - does not throw", async () => {
      const superStreamConsumer = await client.declareSuperStreamConsumer({ superStream: superStreamName }, (_) => {
        return
      })

      await superStreamConsumer.close()
      await superStreamConsumer.close()

      expect(client.consumerCounts()).to.be.equal(0)
    })

    it("closing a super stream consumer after the client closed one partition consumer - does not throw", async () => {
      const superStreamConsumer = await client.declareSuperStreamConsumer({ superStream: superStreamName }, (_) => {
        return
      })
      const [{ consumer }] = client.getConsumers()
      await client.closeConsumer(consumer.extendedId)
      expect(client.consumerCounts()).to.be.equal(noOfPartitions - 1)

      await superStreamConsumer.close()

      expect(client.consumerCounts()).to.be.equal(0)
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
        (message: Message) => {
          messages.push(message)
        }
      )
      await client.declareSuperStreamConsumer(
        { superStream: superStreamName, consumerRef: "counting-messages" },
        (message: Message) => {
          messages.push(message)
        }
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

  describe("deterministic partitioning", () => {
    beforeEach(async () => {
      noOfPartitions = await rabbit.createSuperStream(superStreamName, 2)
      sender = await roundRobinSender(client, superStreamName, 2)
    })

    it("multiple composite consumers with same consumerRef and deterministic partition key - each consumer should read only messages for its partition", async () => {
      const noOfMessagesPerPartition = 10
      const messages: Message[][] = [[], []]

      const allPartitionKeysAreTheSame = (messagesToCheck: Message[]) => {
        const partitionKeys = messagesToCheck.map((m) => m.applicationProperties?.["partition-key"])
        return partitionKeys.every((k) => k === partitionKeys[0])
      }

      await client.declareSuperStreamConsumer(
        { superStream: superStreamName, consumerRef: "message-partitioning" },
        (message: Message) => {
          messages[0].push(message)
        }
      )
      await client.declareSuperStreamConsumer(
        { superStream: superStreamName, consumerRef: "message-partitioning" },
        (message: Message) => {
          messages[1].push(message)
        }
      )

      await sender(noOfMessagesPerPartition)

      await eventually(() => {
        expect(messages[0]).to.have.length(noOfMessagesPerPartition)
        expect(allPartitionKeysAreTheSame(messages[0]))

        expect(messages[1]).to.have.length(noOfMessagesPerPartition)
        expect(allPartitionKeysAreTheSame(messages[1]))
      })
    })
  })

  describe("closing a partition consumer", () => {
    beforeEach(async () => {
      noOfPartitions = await rabbit.createSuperStream(superStreamName, 2)
      sender = await roundRobinSender(client, superStreamName, noOfPartitions)
    })

    it("unsubscribes it, so that another consumer with the same reference can take over that partition", async () => {
      const consumerRef = "close-partition"
      const firstConsumerReceived = new Map<string, number>()
      const secondConsumerReceived = new Map<string, number>()
      const count = (into: Map<string, number>) => (_message: Message, consumer: Consumer) => {
        into.set(consumer.streamName, (into.get(consumer.streamName) ?? 0) + 1)
      }
      await client.declareSuperStreamConsumer(
        { superStream: superStreamName, consumerRef },
        count(firstConsumerReceived)
      )
      const firstConsumers = client.getConsumers().map(({ consumer }) => consumer)
      expect(firstConsumers).to.have.length(noOfPartitions)
      await client.declareSuperStreamConsumer(
        { superStream: superStreamName, consumerRef },
        count(secondConsumerReceived)
      )

      const allConsumers = client.getConsumers().map(({ consumer }) => consumer)
      expect(allConsumers).to.have.length(noOfPartitions * 2)
      const secondConsumers = allConsumers.filter((c) => !firstConsumers.includes(c))
      expect(secondConsumers).to.have.length(noOfPartitions)

      // sending a probe to figure out which consumers are assigned to which partitions
      await sender(1)
      await eventually(() => {
        for (const { streamName } of firstConsumers) {
          expect((firstConsumerReceived.get(streamName) ?? 0) + (secondConsumerReceived.get(streamName) ?? 0)).to.equal(
            1
          )
        }
      })

      const assignedConsumers = firstConsumers
        .filter(({ streamName }) => firstConsumerReceived.has(streamName))
        .concat(secondConsumers.filter(({ streamName }) => secondConsumerReceived.has(streamName)))
      expect(assignedConsumers).to.have.length(noOfPartitions)
      firstConsumerReceived.clear()
      secondConsumerReceived.clear()

      const closedConsumer = assignedConsumers[0]
      await closedConsumer.close()

      expect(client.consumerCounts()).to.be.equal(noOfPartitions * 2 - 1)
      await sender(5)

      const closedReceived = firstConsumers.includes(closedConsumer) ? firstConsumerReceived : secondConsumerReceived
      const assertTakenOver = () => {
        for (const { streamName } of firstConsumers) {
          // expected may include the probe message sent first
          const expected = streamName === closedConsumer.streamName ? 6 : 5
          expect((firstConsumerReceived.get(streamName) ?? 0) + (secondConsumerReceived.get(streamName) ?? 0)).to.equal(
            expected
          )
        }
        expect(closedReceived.get(closedConsumer.streamName)).to.equal(undefined)
      }
      await eventually(assertTakenOver)
      await waitSleeping(300)
      assertTakenOver()
    })
  })
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

const roundRobinSender = async (client: Client, superStreamName: string, partitions: number) => {
  const routingKeyExtractor = (_content: string, msgOptions: MessageOptions) =>
    msgOptions.applicationProperties?.["partition-key"]?.toString()
  const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName }, routingKeyExtractor)

  const sendMessages = async (noOfMessagesPerPartition: number) => {
    for (let i = 0; i < noOfMessagesPerPartition; i++) {
      for (let p = 0; p < partitions; p++) {
        await publisher.send(Buffer.from(`${testMessageContent}-${i * partitions + p}`), {
          applicationProperties: { "partition-key": `${p}` },
        })
      }
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
