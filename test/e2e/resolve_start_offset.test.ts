import { expect } from "chai"
import { Client, Offset, resolveStartOffset } from "../../src"
import { Message } from "../../src/publisher"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"
import { range } from "../../src/util"

describe("resolveStartOffset", () => {
  const rabbit = new Rabbit(username, password)
  let client: Client
  let streamName: string

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    await client.createStream({ stream: streamName, arguments: {} })
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
    } catch (e) {}
  })

  it("no offset stored on the server - returns the default offset", async () => {
    const offset = await resolveStartOffset(client, Offset.first(), "never-stored-ref", streamName)

    expect(offset).to.eql(Offset.first())
  })

  it("an offset is stored on the server - returns the offset after the stored one", async () => {
    const consumerRef = "stored-ref"
    const publisher = await client.declarePublisher({ stream: streamName })
    for (let i = 0; i < 3; i++) {
      await publisher.send(Buffer.from(`Hello ${i}`))
    }
    const received: Message[] = []
    const consumer = await client.declareConsumer(
      { stream: streamName, offset: Offset.first(), consumerRef },
      async (msg: Message) => {
        received.push(msg)
      }
    )
    await eventually(() => {
      expect(received).to.have.length(3)
    })
    await consumer.storeOffset(1n)
    await consumer.close()

    const offset = await resolveStartOffset(client, Offset.first(), consumerRef, streamName)

    expect(offset).to.eql(Offset.offset(2n))
  })

  describe("as the consumerUpdateListener of a super stream consumer", () => {
    let superStreamName: string
    let noOfPartitions: number

    beforeEach(async () => {
      superStreamName = createStreamName()
      noOfPartitions = await rabbit.createSuperStream(superStreamName)
    })

    afterEach(async () => {
      try {
        await rabbit.deleteSuperStream(superStreamName)
      } catch (e) {}
    })

    const publishMessages = async (contents: string[]) => {
      const publisher = await client.declareSuperStreamPublisher(
        { superStream: superStreamName },
        (_, opts) => opts.messageProperties?.messageId
      )
      for (const c of contents) {
        await publisher.send(Buffer.from(c), { messageProperties: { messageId: c } })
      }
      await publisher.close()
    }

    const consume = async (consumerRef: string) => {
      const received: string[] = []
      const consumer = await client.declareSuperStreamConsumer(
        {
          superStream: superStreamName,
          consumerRef,
          consumerUpdateListener: (ref, partition) => resolveStartOffset(client, Offset.first(), ref, partition),
        },
        async (msg, partitionConsumer) => {
          received.push(msg.content.toString())
          await partitionConsumer.storeOffset(msg.offset)
        }
      )
      return { received, consumer }
    }

    it("each partition resumes from its own stored offset", async () => {
      const consumerRef = "super-stream-resume-ref"
      const firstBatch = range(noOfPartitions * 4).map((i) => `first-${i}`)
      const secondBatch = range(noOfPartitions * 2).map((i) => `second-${i}`)
      await publishMessages(firstBatch)
      const first = await consume(consumerRef)
      await eventually(() => {
        expect(first.received).to.have.length(firstBatch.length)
      }, 5000)
      await first.consumer.close()
      await publishMessages(secondBatch)

      const second = await consume(consumerRef)

      await eventually(() => {
        expect(second.received.sort()).to.eql([...secondBatch].sort())
      }, 5000)
      await second.consumer.close()
    }).timeout(10000)
  })
})
