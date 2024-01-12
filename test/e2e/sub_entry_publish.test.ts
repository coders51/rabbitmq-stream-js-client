import { expect } from "chai"
import { Client, Publisher } from "../../src"
import { createClient, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"
import { CompressionType } from "../../src/compression"

describe("publish a batch of messages", () => {
  const rabbit = new Rabbit(username, password)
  let client: Client
  let streamName: string
  let publisher: Publisher

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
    publisher = await createPublisher(streamName, client)
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("publish a batch of messages - without compression", async () => {
    const messages = [
      { content: Buffer.from("Ciao") },
      { content: Buffer.from("Ciao1") },
      { content: Buffer.from("Ciao2") },
      { content: Buffer.from("Ciao3") },
    ]

    await publisher.sendSubEntries(messages)

    await eventually(async () => {
      const info = await rabbit.getQueueInfo(streamName)
      expect(info.messages).eql(messages.length)
    }, 10000)
  }).timeout(10000)

  it("publish a batch of messages with compression", async () => {
    const messages = [
      { content: Buffer.from("Ciao") },
      { content: Buffer.from("Ciao1") },
      { content: Buffer.from("Ciao2") },
      { content: Buffer.from("Ciao3") },
    ]

    await publisher.sendSubEntries(messages, CompressionType.Gzip)

    await eventually(async () => {
      const info = await rabbit.getQueueInfo(streamName)
      expect(info.messages).eql(messages.length)
    }, 10000)
  }).timeout(10000)
})
