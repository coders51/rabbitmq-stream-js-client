import { Client, Offset } from "../../src"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { username, password } from "../support/util"

describe("close client", () => {
  const rabbit = new Rabbit(username, password)
  let streamName: string
  let client: Client

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    try {
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("can close client after closing publisher", async () => {
    const publisher = await client.declarePublisher({ stream: streamName })

    await publisher.close()
    await client.close()
  })

  it("can close client after closing consumer", async () => {
    const consumer = await client.declareConsumer({ stream: streamName, offset: Offset.first() }, (_msg) => {
      /* nothing */
    })

    await consumer.close()
    await client.close()
  })
})
