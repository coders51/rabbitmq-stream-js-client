import { expect } from "chai"
import { randomUUID } from "crypto"
import { Client } from "../../src"
import { Message, MessageOptions } from "../../src/publisher"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync, password, username, wait } from "../support/util"

describe("super stream publisher", () => {
  let superStreamName: string
  const rabbit = new Rabbit(username, password)
  let client: Client
  let noOfPartitions: number = 0

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

  it("declare a superstream publisher on an existent stream - should not throw", async () => {
    await client.declareSuperStreamPublisher({ superStream: superStreamName }, (_) => {
      return ""
    })
  })

  it("declare a superstream publisher on a non-existent stream - should throw", async () => {
    await expectToThrowAsync(
      async () => {
        await client.declareSuperStreamPublisher({ superStream: createStreamName() }, (_) => {
          return ""
        })
      },
      Error,
      /Stream does not exist/
    )
  })

  it("the actual publishers gets created lazily - without send, no publisher is actually instantiated", async () => {
    await client.declareSuperStreamPublisher({ superStream: superStreamName }, (_) => {
      return ""
    })

    await wait(1000)
    expect(await rabbit.returnPublishers(`${superStreamName}-0`)).to.be.empty
  }).timeout(3000)

  it("publish a message without the routing key throws", async () => {
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName },
      (_, opts: MessageOptions) => {
        return opts.messageProperties?.messageId
      }
    )

    await expectToThrowAsync(
      async () => {
        await publisher.send(Buffer.from("Hello world"), {})
      },
      Error,
      /Routing key is empty or undefined with the provided extractor/
    )
  })

  it("publish a message with an empty routing key throws", async () => {
    const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName }, (_) => {
      return ""
    })

    await expectToThrowAsync(
      async () => {
        await publisher.send(Buffer.from("Hello world"), {})
      },
      Error,
      /Routing key is empty or undefined with the provided extractor/
    )
  })

  it("publish a message and receive a message", async () => {
    const messages: Message[] = []
    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (msg) => {
      messages.push(msg)
    })
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName },
      (_, opts: MessageOptions) => {
        return opts.messageProperties?.messageId
      }
    )

    await publisher.send(Buffer.from("Hello world"), { messageProperties: { messageId: "1" } })

    await eventually(() => {
      expect(messages).to.have.length(1)
    }, 2000)
  })

  it("publish a message and receive a message when specifying the publishing id", async () => {
    const messages: Message[] = []
    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (msg) => {
      messages.push(msg)
    })
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName, publisherRef: "publisher-ref" },
      (_, opts: MessageOptions) => {
        return opts.messageProperties?.messageId
      }
    )

    await publisher.basicSend(1n, Buffer.from("Hello world"), { messageProperties: { messageId: "1" } })

    await eventually(async () => {
      expect(messages).to.have.length(1)
      expect(await publisher.getLastPublishingId()).to.be.eql(1n)
    }, 2000)
  }).timeout(5000)

  it("publish several messages - they should be routed to different partitions", async () => {
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName },
      (_, opts: MessageOptions) => {
        return opts.messageProperties?.messageId
      }
    )

    for (let i = 0; i < noOfPartitions * 3; i++) {
      await publisher.send(Buffer.from(`Hello world ${i}`), {
        messageProperties: { messageId: randomUUID() },
      })
    }

    await eventually(() => {
      expect(client.publisherCounts()).to.be.eql(noOfPartitions)
    }, 3000)
  }).timeout(5000)

  it("closing the superstream publisher closes all connections besides the locator", async () => {
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName },
      (_, opts: MessageOptions) => {
        return opts.messageProperties?.messageId
      }
    )
    for (let i = 0; i < noOfPartitions * 2; i++) {
      await publisher.send(Buffer.from(`Hello world ${i}`), {
        messageProperties: { messageId: randomUUID() },
      })
    }

    await publisher.close()

    await eventually(async () => {
      const connections = await rabbit.getConnections()
      expect(connections).to.have.length(1)
    }, 5000)
  }).timeout(5000)

  it("closing the locator closes all connections", async () => {
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName },
      (_, opts: MessageOptions) => {
        return opts.messageProperties?.messageId
      }
    )
    for (let i = 0; i < noOfPartitions * 2; i++) {
      await publisher.send(Buffer.from(`Hello world ${i}`), {
        messageProperties: { messageId: randomUUID() },
      })
    }

    await client.close()

    await eventually(async () => {
      const connections = await rabbit.getConnections()
      expect(connections).to.have.length(0)
    }, 5000)
  }).timeout(5000)

  it("check the hashing algorithm - if it's properly implemented, the following should hold", async () => {
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName },
      (_, opts) => opts.messageProperties?.messageId
    )

    for (let i = 0; i < 20; i++) {
      await publisher.basicSend(BigInt(i), Buffer.from("hello"), { messageProperties: { messageId: `hello${i}` } })
    }

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(`${superStreamName}-0`)).messages).to.eql(9)
      expect((await rabbit.getQueueInfo(`${superStreamName}-1`)).messages).to.eql(7)
      expect((await rabbit.getQueueInfo(`${superStreamName}-2`)).messages).to.eql(4)
    }, 10000)
  }).timeout(12000)

  it("superstream publishing with key routing strategy - select an existing partition", async () => {
    const messages: Message[] = []
    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (msg) => {
      messages.push(msg)
    })
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName, routingStrategy: "key" },
      (_) => {
        return "0"
      }
    )

    for (let i = 0; i < noOfPartitions * 2; i++) {
      await publisher.send(Buffer.from(`Hello world ${i}`), {
        messageProperties: { messageId: randomUUID() },
      })
    }

    await eventually(() => {
      expect(messages).to.have.length(noOfPartitions * 2)
    })
  })

  it("superstream publishing with key routing strategy - the routingKey is not an existing bindingKey for the superstream", async () => {
    const messages: Message[] = []
    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (msg) => {
      messages.push(msg)
    })
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName, routingStrategy: "key" },
      (_) => {
        return "non-existent"
      }
    )
    await expectToThrowAsync(
      async () => {
        await publisher.send(Buffer.from(`I will break`), {
          messageProperties: { messageId: randomUUID() },
        })
      },
      Error,
      /The server did not return any partition for routing key/
    )
  })
})
