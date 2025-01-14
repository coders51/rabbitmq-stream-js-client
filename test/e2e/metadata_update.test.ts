import { expect } from "chai"
import { Client, Offset } from "../../src"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"

describe("react to a metadata update message from the server", () => {
  const rabbit = new Rabbit(username, password)
  let client: Client
  let streamName: string

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async function () {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {
      console.error("Error while trying to clean up Rabbit's state after testing", e)
    }
  })

  it("when we have a metadata update on a stream any consumer on that stream gets removed from the consumers list", async () => {
    await client.declareConsumer({ offset: Offset.first(), stream: streamName }, () => {
      return
    })

    await rabbit.deleteStream(streamName)

    await eventually(() => {
      expect(client.consumerCounts()).to.eql(0)
    }, 3000)
  })

  it("when we have a metadata update on a stream the connection closed callback of its consumers fires", async () => {
    let cbCalled = 0
    await client.declareConsumer(
      { offset: Offset.first(), stream: streamName, connectionClosedListener: (_) => cbCalled++ },
      () => {
        return
      }
    )

    await rabbit.deleteStream(streamName)

    await eventually(() => {
      expect(client.consumerCounts()).to.eql(0)
      expect(cbCalled).to.eql(1)
    }, 3000)
  }).timeout(5000)

  it("when we have a metadata update on a stream any publisher on that stream gets closed", async () => {
    const publisher = await client.declarePublisher({ stream: streamName })

    await rabbit.deleteStream(streamName)

    await eventually(() => {
      expect(client.publisherCounts()).to.eql(0)
      expect(publisher.closed).to.eql(true)
    }, 3000)
  })

  it("when we have a metadata update on a stream the connection closed callback of its publishers fires", async () => {
    let cbCalled = 0
    await client.declarePublisher({ stream: streamName, connectionClosedListener: (_) => cbCalled++ })

    await rabbit.deleteStream(streamName)

    await eventually(() => {
      expect(cbCalled).to.eql(1)
    }, 3000)
  })
})
