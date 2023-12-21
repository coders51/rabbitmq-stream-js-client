import { expect } from "chai"
import { Client } from "../../src"
import { createClient, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync, username, password } from "../support/util"

describe("declare publisher", () => {
  let streamName: string
  let nonExistingStreamName: string
  const rabbit = new Rabbit(username, password)
  let client: Client

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    nonExistingStreamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("declaring a publisher on an existing stream - the publisher should be created", async () => {
    const producer = await createPublisher(streamName, client)

    await eventually(async () => {
      expect(await rabbit.returnPublishers(streamName))
        .lengthOf(1)
        .and.to.include(producer.ref)
    }, 5000)
  }).timeout(10000)

  it("declaring a publisher on an existing stream with no publisherRef - the publisher should be created", async () => {
    const producer = await createPublisher(streamName, client)

    await eventually(async () => {
      expect(await rabbit.returnPublishers(streamName))
        .lengthOf(1)
        .and.to.include(producer.ref)
    }, 5000)
  }).timeout(10000)

  it("declaring a publisher on a non-existing stream should raise an error", async () => {
    await expectToThrowAsync(
      () => createPublisher(nonExistingStreamName, client),
      Error,
      "Stream was not found on any node"
    )
  })
})
