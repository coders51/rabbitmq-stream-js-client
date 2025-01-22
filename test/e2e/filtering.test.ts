import { expect } from "chai"
import { randomUUID } from "crypto"
import { Client, Offset } from "../../src"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"
import { coerce, lt } from "semver"

describe("filtering", () => {
  const rabbit = new Rabbit(username, password)
  let client: Client
  let streamName: string

  beforeEach(async function () {
    client = await createClient(username, password)
    // eslint-disable-next-line no-invalid-this
    if (lt(coerce(client.rabbitManagementVersion)!, "3.13.0")) this.skip()
    streamName = createStreamName()
    await client.createStream({ stream: streamName, arguments: {} })
  })

  afterEach(async () => {
    try {
      await client.close()
      await client.deleteStream({ stream: streamName })
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("can publish with filter value", async () => {
    const publisher = await client.declarePublisher(
      { stream: streamName, publisherRef: `my-publisher-${randomUUID()}` },
      (msg) => msg.applicationProperties!["test"].toString()
    )
    const message1 = "test1"
    const message2 = "test2"
    const message3 = "test3"
    const applicationProperties1 = { test: "A" }
    const applicationProperties2 = { test: "B" }

    await publisher.send(Buffer.from(message1), { applicationProperties: applicationProperties1 })
    await publisher.send(Buffer.from(message2), { applicationProperties: applicationProperties1 })
    await publisher.send(Buffer.from(message3), { applicationProperties: applicationProperties2 })

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(streamName)).messages).eql(3)
    }, 10000)
  }).timeout(10000)

  it("published messages are filtered on the consumer side", async () => {
    const filteredMsg: string[] = []
    const publisher = await client.declarePublisher(
      { stream: streamName, publisherRef: `my-publisher-${randomUUID()}` },
      (msg) => msg.applicationProperties!["test"].toString()
    )
    const message1 = "test1"
    const message2 = "test2"
    const message3 = "test3"
    const applicationProperties1 = { test: "A" }
    const applicationProperties2 = { test: "B" }
    const applicationProperties3 = { test: "C" }
    await publisher.send(Buffer.from(message1), { applicationProperties: applicationProperties1 })
    await publisher.send(Buffer.from(message2), { applicationProperties: applicationProperties2 })
    await publisher.send(Buffer.from(message3), { applicationProperties: applicationProperties3 })

    await client.declareConsumer(
      {
        stream: streamName,
        offset: Offset.first(),
        filter: {
          values: ["A", "B"],
          postFilterFunc: (msg) => msg.applicationProperties!["test"] === "A",
          matchUnfiltered: true,
        },
      },
      (msg) => {
        filteredMsg.push(msg.content.toString("utf-8"))
      }
    )

    await eventually(async () => {
      expect(filteredMsg[0]).eql("test1")
      expect(filteredMsg.length).eql(1)
    }, 10000)
  }).timeout(10000)

  it("published messages are filtered on the server side keeping only the ones with filter value", async () => {
    const expectedMessages: string[] = []
    const notCorrectlyFilteredMessages: string[] = []
    const publisher = await client.declarePublisher(
      { stream: streamName, publisherRef: `my-publisher-${randomUUID()}` },
      (msg) => (msg.applicationProperties ? msg.applicationProperties["test"].toString() : undefined)
    )
    const applicationProperties1 = { test: "A" }
    const applicationProperties2 = { test: "B" }
    for (let i = 0; i < 1000; i++)
      await publisher.send(Buffer.from(`test${i + 1}`), { applicationProperties: applicationProperties1 })
    for (let i = 0; i < 1000; i++)
      await publisher.send(Buffer.from(`test${i + 1}`), { applicationProperties: applicationProperties2 })
    for (let i = 0; i < 1000; i++) await publisher.send(Buffer.from(`test${i + 1}`))

    await client.declareConsumer(
      {
        stream: streamName,
        offset: Offset.first(),
        filter: {
          values: ["A", "B"],
          postFilterFunc: (_msg) => true,
          matchUnfiltered: false,
        },
      },
      (msg) => {
        if (msg.applicationProperties?.test === "A" || msg.applicationProperties?.test === "B")
          expectedMessages.push(msg.content.toString("utf-8"))
        else notCorrectlyFilteredMessages.push(msg.content.toString("utf-8"))
      }
    )

    //RabbitMQ uses a Bloom filter for server side filtering.
    //A Bloom filter is very efficient in terms of storage and speed, but it is probabilistic: it can return false positives.
    //Because of this, the broker can send messages it believes match the expected filter values whereas they do not. That's why some client-side filtering logic is necessary.
    //For this reason some messages may not be correctly filtered, but we expect the number of them to be very low.
    //For more information: https://www.rabbitmq.com/blog/2023/10/16/stream-filtering
    await eventually(async () => {
      expect(expectedMessages.length).eql(2000)
      expect(notCorrectlyFilteredMessages.length).below(150)
    }, 10000)
  }).timeout(15000)

  it("published messages are filtered on the server side keeping even the ones with filter value", async () => {
    const filteredMsg: string[] = []
    const publisher = await client.declarePublisher(
      { stream: streamName, publisherRef: `my-publisher-${randomUUID()}` },
      (msg) => (msg.applicationProperties ? msg.applicationProperties["test"].toString() : undefined)
    )
    const applicationProperties1 = { test: "A" }
    const applicationProperties2 = { test: "B" }
    for (let i = 0; i < 1000; i++)
      await publisher.send(Buffer.from(`test${i + 1}`), { applicationProperties: applicationProperties1 })
    for (let i = 0; i < 1000; i++)
      await publisher.send(Buffer.from(`test${i + 1}`), { applicationProperties: applicationProperties2 })
    for (let i = 0; i < 1000; i++) await publisher.send(Buffer.from(`test${i + 1}`))

    await client.declareConsumer(
      {
        stream: streamName,
        offset: Offset.first(),
        filter: {
          values: ["A", "B"],
          postFilterFunc: (_msg) => true,
          matchUnfiltered: true,
        },
      },
      (msg) => {
        filteredMsg.push(msg.content.toString("utf-8"))
      }
    )

    await eventually(async () => {
      expect(filteredMsg.length).eql(3000)
    }, 10000)
  }).timeout(10000)
})
