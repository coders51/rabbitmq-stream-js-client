import { expect } from "chai"
import { Offset } from "../../src"
import { Message } from "../../src/publisher"
import { createClient, createStreamName } from "../support/fake_data"
import { eventually, password, username } from "../support/util"

describe("Consumer Offset", () => {
  it("test start and stop", async () => {
    const client = await createClient(username, password)

    const streamName = createStreamName()
    await client.createStream({ stream: streamName, arguments: {} })

    let onIncomingMessageCalls = 0
    const onIncomingMessage = async (msg: Message) => {
      console.log(msg.content.toString("utf-8"))
      console.log(msg.offset)
      onIncomingMessageCalls++
      return
    }
    const referenceName = "ref"
    const consumer = await client.declareConsumer(
      {
        stream: streamName,
        offset: Offset.offset(0n),
        consumerRef: referenceName,
      },
      onIncomingMessage
    )
    const publisher = await client.declarePublisher({ stream: streamName })
    await publisher.send(Buffer.from("Hello1"))
    await eventually(async () => {
      expect(onIncomingMessageCalls).to.eql(1)
    })

    const localOffset = consumer.getOffset()
    if (localOffset === undefined) {
      throw new Error("localOffset is undefined")
    }

    // Perhaps there may be an option to upload the offset to the server directly from the consumer's internal store? Instead of having to fetch for it and then retrieve it
    await consumer.storeOffset(localOffset)
    await consumer.close(false)

    await publisher.send(Buffer.from("Hello2"))
    await publisher.send(Buffer.from("Hello3"))

    const lastMessageOffset = await client.queryOffset({
      stream: streamName,
      reference: referenceName,
    })
    expect(lastMessageOffset).to.eql(0n)

    let resumedOnIncomingMessageCalls = 0
    let offset: bigint | undefined = 0n
    const resumedOnIncomingMessage = async (msg: Message) => {
      console.log("Resumed ", msg.content.toString("utf-8"))
      offset = msg.offset
      resumedOnIncomingMessageCalls++
      return
    }

    const resumedConsumer = await client.declareConsumer(
      {
        stream: streamName,
        offset: Offset.offset(lastMessageOffset + 1n),
        consumerRef: referenceName,
      },
      resumedOnIncomingMessage
    )
    await eventually(async () => {
      expect(resumedOnIncomingMessageCalls).to.eql(2)
    })
    expect(resumedConsumer.getOffset()).to.eql(offset)

    await publisher.close(false)
  })
})
