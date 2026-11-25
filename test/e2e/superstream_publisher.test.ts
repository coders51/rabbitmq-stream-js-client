import { use as chaiUse, expect, spy } from "chai"
import spies from "chai-spies"
import { randomUUID } from "crypto"
import { Client } from "../../src"
import { Message, MessageOptions } from "../../src/publisher"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync, password, username, wait } from "../support/util"
import { CompressionType } from "../../src/compression"
import { range } from "../../src/util"

chaiUse(spies)

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
        messageProperties: { messageId: `${i}` },
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

  it("key routing strategy - the routing key is resolved only once thanks to the cache", async () => {
    const sandbox = spy.sandbox()
    try {
      sandbox.on(client, "routeQuery")
      const publisher = await client.declareSuperStreamPublisher(
        { superStream: superStreamName, routingStrategy: "key" },
        () => "0"
      )

      await publisher.send(Buffer.from("first"), {})
      await publisher.send(Buffer.from("second"), {})

      expect(client.routeQuery).to.have.been.called.once
    } finally {
      sandbox.restore()
    }
  })

  it("key routing strategy - routingCacheSize bounds the cache", async () => {
    const sandbox = spy.sandbox()
    try {
      sandbox.on(client, "routeQuery")
      const publisher = await client.declareSuperStreamPublisher(
        { superStream: superStreamName, routingStrategy: "key", routingCacheSize: 1 },
        (content) => content
      )

      await publisher.send(Buffer.from("0"), {})
      await publisher.send(Buffer.from("1"), {})
      await publisher.send(Buffer.from("0"), {})

      expect(client.routeQuery).to.have.been.called.exactly(3)
    } finally {
      sandbox.restore()
    }
  })

  it("key routing strategy - a custom routingCache is used instead of the built-in one", async () => {
    const entries = new Map<string, string>()
    const cache = {
      get: (key: string) => entries.get(key),
      set: (key: string, value: string) => {
        entries.set(key, value)
      },
    }
    const sandbox = spy.sandbox()
    try {
      sandbox.on(client, "routeQuery")
      const publisher = await client.declareSuperStreamPublisher(
        { superStream: superStreamName, routingStrategy: "key", routingCache: cache, routingCacheSize: 0 },
        () => "0"
      )

      await publisher.send(Buffer.from("first"), {})
      await publisher.send(Buffer.from("second"), {})

      expect(client.routeQuery).to.have.been.called.once
      expect([...entries.keys()]).to.eql([`${superStreamName}:0`])
    } finally {
      sandbox.restore()
    }
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

  it("send uses the routingKey from the message - no key extractor needed", async () => {
    const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName })

    for (let i = 0; i < 20; i++) {
      await publisher.send(Buffer.from("hello"), { routingKey: `hello${i}` })
    }

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(`${superStreamName}-0`)).messages).to.eql(9)
      expect((await rabbit.getQueueInfo(`${superStreamName}-1`)).messages).to.eql(7)
      expect((await rabbit.getQueueInfo(`${superStreamName}-2`)).messages).to.eql(4)
    }, 10000)
  }).timeout(12000)

  it("send prefers the routingKey from the message over the key extractor", async () => {
    const messages: Message[] = []
    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (msg) => {
      messages.push(msg)
    })
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName, routingStrategy: "key" },
      (_) => "non-existent"
    )

    await publisher.send(Buffer.from("Hello world"), { routingKey: "0" })

    await eventually(() => {
      expect(messages).to.have.length(1)
    }, 2000)
  })

  it("send without routingKey in the message and without key extractor throws", async () => {
    const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName })

    await expectToThrowAsync(
      async () => {
        await publisher.send(Buffer.from("Hello world"), {})
      },
      Error,
      /Routing key is empty or undefined with the provided extractor/
    )
  })

  it("send with an empty string routingKey throws and does not fall back to the key extractor", async () => {
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName },
      (_) => "valid-key-from-extractor"
    )

    await expectToThrowAsync(
      async () => {
        await publisher.send(Buffer.from("Hello world"), { routingKey: "" })
      },
      Error,
      /Routing key is empty or undefined with the provided extractor/
    )
  })

  it("sendSubEntries with an empty string routingKey throws and does not fall back to the key extractor", async () => {
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName },
      (_) => "valid-key-from-extractor"
    )

    await expectToThrowAsync(
      async () => {
        await publisher.sendSubEntries([{ content: Buffer.from("Hello world"), routingKey: "" }], CompressionType.None)
      },
      Error,
      /Routing key is empty or undefined with the provided extractor/
    )
  })

  it("basicSend uses the routingKey from the message - no key extractor needed", async () => {
    const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName })

    for (let i = 0; i < 20; i++) {
      await publisher.basicSend(BigInt(i), Buffer.from("hello"), { routingKey: `hello${i}` })
    }

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(`${superStreamName}-0`)).messages).to.eql(9)
      expect((await rabbit.getQueueInfo(`${superStreamName}-1`)).messages).to.eql(7)
      expect((await rabbit.getQueueInfo(`${superStreamName}-2`)).messages).to.eql(4)
    }, 10000)
  }).timeout(12000)

  it("basicSend prefers the routingKey from the message over the key extractor", async () => {
    const messages: Message[] = []
    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (msg) => {
      messages.push(msg)
    })
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName, routingStrategy: "key", publisherRef: "publisher-ref" },
      (_) => "non-existent"
    )

    await publisher.basicSend(1n, Buffer.from("Hello world"), { routingKey: "0" })

    await eventually(async () => {
      expect(messages).to.have.length(1)
      expect(await publisher.getLastPublishingId()).to.be.eql(1n)
    }, 2000)
  })

  it("basicSend without routingKey in the message and without key extractor throws", async () => {
    const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName })

    await expectToThrowAsync(
      async () => {
        await publisher.basicSend(1n, Buffer.from("Hello world"), {})
      },
      Error,
      /Routing key is empty or undefined with the provided extractor/
    )
  })

  it("basicSend with an empty string routingKey throws and does not fall back to the key extractor", async () => {
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName },
      (_) => "valid-key-from-extractor"
    )

    await expectToThrowAsync(
      async () => {
        await publisher.basicSend(1n, Buffer.from("Hello world"), { routingKey: "" })
      },
      Error,
      /Routing key is empty or undefined with the provided extractor/
    )
  })

  it("sendSubEntries uses the routingKey from each message - no key extractor needed", async () => {
    const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName })
    const messages = range(20).map((_, i) => ({ content: Buffer.from("hello"), routingKey: `hello${i}` }))

    await publisher.sendSubEntries(messages, CompressionType.None)

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(`${superStreamName}-0`)).messages).to.eql(9)
      expect((await rabbit.getQueueInfo(`${superStreamName}-1`)).messages).to.eql(7)
      expect((await rabbit.getQueueInfo(`${superStreamName}-2`)).messages).to.eql(4)
    }, 10000)
  }).timeout(12000)

  it("sendSubEntries prefers the routingKey from the message over the key extractor", async () => {
    const receivedMessages: Message[] = []
    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (m: Message) => {
      receivedMessages.push(m)
    })
    const publisher = await client.declareSuperStreamPublisher(
      { superStream: superStreamName, routingStrategy: "key" },
      (_) => "non-existent"
    )
    const messages = range(5).map((_, i) => ({ content: Buffer.from(`Hello world ${i}`), routingKey: "0" }))

    await publisher.sendSubEntries(messages, CompressionType.Gzip)

    await eventually(() => {
      expect(receivedMessages).to.have.length(messages.length)
    }, 5000)
  })

  it("publishing a batch of messages without compression - should not raise error", async () => {
    const messageContents = range(5).map((_, i) => `Hello world ${i}`)
    const messages = messageContents.map((m, i) => ({
      content: Buffer.from(m),
      messageProperties: { messageId: `${i}` },
    }))
    const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName }, (_, opts) => {
      return opts.messageProperties?.messageId ?? "-1"
    })

    await publisher.sendSubEntries(messages, CompressionType.None)
  })

  it("publishing a batch of messages without compression - receive the same number of messages", async () => {
    const receivedMessages: Message[] = []
    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (m: Message) => {
      receivedMessages.push(m)
    })
    const messageContents = range(5).map((_, i) => `Hello world ${i}`)
    const messages = messageContents.map((m, i) => ({
      content: Buffer.from(m),
      messageProperties: { messageId: `${i}` },
    }))
    const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName }, (_, opts) => {
      return opts.messageProperties?.messageId ?? "-1"
    })

    await publisher.sendSubEntries(messages, CompressionType.None)

    await eventually(async () => {
      expect(receivedMessages.length).eql(messages.length)
    }, 5000)
  })

  it("publishing a batch of messages with compression - should not raise error", async () => {
    const messageContents = range(5).map((_, i) => `Hello world ${i}`)
    const messages = messageContents.map((m, i) => ({
      content: Buffer.from(m),
      messageProperties: { messageId: `${i}` },
    }))
    const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName }, (_, opts) => {
      return opts.messageProperties?.messageId ?? "-1"
    })

    await publisher.sendSubEntries(messages, CompressionType.Gzip)
  })

  it("publishing a batch of messages with compression - receive the same number of messages", async () => {
    const receivedMessages: Message[] = []
    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (m: Message) => {
      receivedMessages.push(m)
    })
    const messageContents = range(5).map((_, i) => `Hello world ${i}`)
    const messages = messageContents.map((m, i) => ({
      content: Buffer.from(m),
      messageProperties: { messageId: `${i}` },
    }))
    const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName }, (_, opts) => {
      return opts.messageProperties?.messageId ?? "-1"
    })

    await publisher.sendSubEntries(messages, CompressionType.Gzip)

    await eventually(async () => {
      expect(receivedMessages.length).eql(messages.length)
    }, 5000)
  })

  it("publishing a batch of messages with compression - content is readable", async () => {
    const receivedMessages: Message[] = []
    await client.declareSuperStreamConsumer({ superStream: superStreamName }, (m: Message) => {
      receivedMessages.push(m)
    })
    const messageContents = range(5).map((_, i) => `Hello world ${i}`)
    const messages = messageContents.map((m, i) => ({
      content: Buffer.from(m),
      messageProperties: { messageId: `${i}` },
    }))
    const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName }, (_, opts) => {
      return opts.messageProperties?.messageId ?? "-1"
    })

    await publisher.sendSubEntries(messages, CompressionType.Gzip)

    await eventually(async () => {
      for (const rm of receivedMessages) {
        expect(rm.content.toString()).to.match(/Hello world \d/)
      }
    }, 5000)
  })
})
