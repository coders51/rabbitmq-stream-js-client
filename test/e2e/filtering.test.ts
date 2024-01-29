import { expect } from "chai"
import { randomUUID } from "crypto"
import { Client } from "../../src"
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
})
