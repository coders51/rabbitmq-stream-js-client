import { use as chaiUse, expect, spy } from "chai"
import { Client, Publisher } from "../../src"
import { Message } from "../../src/publisher"
import { Offset } from "../../src/requests/subscribe_request"
import { createClient, createConsumerRef, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { always, eventually, password, username } from "../support/util"
import {
  creditsOnChunkCompleted,
  creditsOnChunkProgress,
  creditsOnChunkReceived,
} from "../../src/consumer_credit_policy"
import spies from "chai-spies"
chaiUse(spies)

const send = async (publisher: Publisher, chunks: Message[][]) => {
  for (const chunk of chunks) {
    for (const msg of chunk) {
      await publisher.send(msg.content)
    }
    await publisher.flush()
  }
}

describe("consumer credit flow policies", () => {
  let streamName: string
  const rabbit = new Rabbit(username, password)
  let client: Client
  let publisher: Publisher
  const previousMaxSharedClientInstances = process.env.MAX_SHARED_CLIENT_INSTANCES
  const sandbox = spy.sandbox()
  const chunk: Message[] = [
    { content: Buffer.from("hello") },
    { content: Buffer.from("there") },
    { content: Buffer.from("brave") },
    { content: Buffer.from("new") },
    { content: Buffer.from("world") },
  ]
  const chunks = [chunk, chunk]
  const nMessages = chunk.length * chunks.length

  before(() => {
    process.env.MAX_SHARED_CLIENT_INSTANCES = "10"
  })

  after(() => {
    if (previousMaxSharedClientInstances !== undefined) {
      process.env.MAX_SHARED_CLIENT_INSTANCES = previousMaxSharedClientInstances
      return
    }
    delete process.env.MAX_SHARED_CLIENT_INSTANCES
  })

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
    publisher = await createPublisher(streamName, client)
  })

  afterEach(async () => {
    try {
      sandbox.restore()
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (_e) {}
  })

  it("NewCreditOnChunkReceived policy requests new credit when chunk is completely received", async () => {
    const consumerRef = createConsumerRef()
    const policy = creditsOnChunkReceived(1, 1)
    sandbox.on(policy, "onChunkReceived")
    const received: Message[] = []
    await client.declareConsumer(
      {
        stream: streamName,
        offset: Offset.first(),
        singleActive: true,
        consumerRef: consumerRef,
        creditPolicy: policy,
      },
      (message: Message) => {
        received.push(message)
      }
    )

    await send(publisher, chunks)

    await eventually(() => expect(received.length).eql(nMessages))
    await eventually(() => expect(policy.onChunkReceived).called.exactly(chunks.length))
    await always(() => expect(policy.onChunkReceived).called.below(chunks.length + 1), 5000)
  }).timeout(10000)

  it("NewCreditOnChunkProgress policy requests new credit when chunk handled at 50% progress", async () => {
    const consumerRef = createConsumerRef()
    const policy = creditsOnChunkProgress(1, 0.5, 1)
    sandbox.on(policy, "onChunkProgress")
    const received: Message[] = []
    await client.declareConsumer(
      {
        stream: streamName,
        offset: Offset.first(),
        singleActive: true,
        consumerRef: consumerRef,
        creditPolicy: policy,
      },
      (message: Message) => {
        received.push(message)
      }
    )

    await send(publisher, chunks)

    await eventually(() => expect(received.length).eql(nMessages))
    await eventually(() => expect(policy.onChunkProgress).called.exactly(nMessages))
    await always(() => expect(policy.onChunkProgress).called.below(nMessages + 1), 5000)
  }).timeout(10000)

  it("NewCreditOnChunkProgress policy requests new credit when chunk handled at 100% progress", async () => {
    const consumerRef = createConsumerRef()
    const policy = creditsOnChunkProgress(1, 1, 1)
    sandbox.on(policy, "onChunkProgress")
    const received: Message[] = []
    await client.declareConsumer(
      {
        stream: streamName,
        offset: Offset.first(),
        singleActive: true,
        consumerRef: consumerRef,
        creditPolicy: policy,
      },
      (message: Message) => {
        received.push(message)
      }
    )

    await send(publisher, chunks)

    await eventually(() => expect(received.length).eql(nMessages))
    await eventually(() => expect(policy.onChunkProgress).called.exactly(nMessages))
    await always(() => expect(policy.onChunkProgress).called.below(nMessages + 1), 5000)
  }).timeout(10000)

  it("NewCreditOnChunkCompleted policy requests new credit when chunk completely handled", async () => {
    const consumerRef = createConsumerRef()
    const policy = creditsOnChunkCompleted(1, 1)
    sandbox.on(policy, "onChunkCompleted")
    const received: Message[] = []
    await client.declareConsumer(
      {
        stream: streamName,
        offset: Offset.first(),
        singleActive: true,
        consumerRef: consumerRef,
        creditPolicy: policy,
      },
      async (message: Message) => {
        received.push(message)
      }
    )

    await send(publisher, chunks)

    await eventually(() => expect(received.length).eql(nMessages))
    await eventually(() => expect(policy.onChunkCompleted).called.exactly(chunks.length))
    await always(() => expect(policy.onChunkCompleted).called.below(chunks.length + 1), 5000)
  }).timeout(10000)
})
