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
    await client.declareSuperStreamPublisher(superStreamName, (_) => {
      return ""
    })
  })

  it("declare a superstream publisher on a non-existent stream - should throw", async () => {
    await expectToThrowAsync(
      async () => {
        await client.declareSuperStreamPublisher(createStreamName(), (_) => {
          return ""
        })
      },
      Error,
      /Stream does not exist/
    )
  })

  it("the actual publishers gets created lazily - without send, no publisher is actually instantiated", async () => {
    await client.declareSuperStreamPublisher(superStreamName, (_) => {
      return ""
    })

    await wait(1000)
    expect(await rabbit.returnPublishers(`${superStreamName}-0`)).to.be.empty
  }).timeout(3000)

  it("publish a message without the routing key throws", async () => {
    const publisher = await client.declareSuperStreamPublisher(superStreamName, (opts: MessageOptions) => {
      return opts.messageProperties?.messageId
    })

    await expectToThrowAsync(
      async () => {
        await publisher.send(Buffer.from("Hello world"), {})
      },
      Error,
      /Routing key is empty or undefined with the provided extractor/
    )
  })

  it("publish a message with an empty routing key throws", async () => {
    const publisher = await client.declareSuperStreamPublisher(superStreamName, (_: MessageOptions) => {
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
    await client.declareSuperStreamConsumer(superStreamName, (msg) => {
      messages.push(msg)
    })
    const publisher = await client.declareSuperStreamPublisher(superStreamName, (opts: MessageOptions) => {
      return opts.messageProperties?.messageId
    })

    await publisher.send(Buffer.from("Hello world"), { messageProperties: { messageId: "1" } })

    await eventually(() => {
      expect(messages).to.have.length(1)
    }, 2000)
  })

  it("publish several messages - they should be routed to different partitions", async () => {
    const publisher = await client.declareSuperStreamPublisher(superStreamName, (opts: MessageOptions) => {
      return opts.messageProperties?.messageId
    })

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
    const publisher = await client.declareSuperStreamPublisher(superStreamName, (opts: MessageOptions) => {
      return opts.messageProperties?.messageId
    })
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
    const publisher = await client.declareSuperStreamPublisher(superStreamName, (opts: MessageOptions) => {
      return opts.messageProperties?.messageId
    })
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
})
