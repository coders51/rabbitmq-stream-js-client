import { expect } from "chai"
import { Client, ListenersParams } from "../../src"
import { MetadataUpdateResponse } from "../../src/responses/metadata_update_response"
import { createClient, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"

describe("update the metadata from the server", () => {
  const rabbit = new Rabbit(username, password)
  let client: Client
  let streamName: string
  const metadataUpdateResponses: MetadataUpdateResponse[] = []

  beforeEach(async () => {
    metadataUpdateResponses.length = 0
    const listeners: ListenersParams = {
      metadata_update: (data) => metadataUpdateResponses.push(data),
    }
    client = await createClient(username, password, listeners)
    streamName = createStreamName()
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

  it("when delete stream we receive a metadataUpdate", async () => {
    await createPublisher(streamName, client)
    await rabbit.deleteStream(streamName)
    await eventually(async () => expect(metadataUpdateResponses.length).greaterThanOrEqual(1), 10000)
  }).timeout(10000)

  it("when delete stream we receive a metadataUpdate registering after creation", async () => {
    let called = 0
    const publisher = await createPublisher(streamName, client)
    publisher.on("metadata_update", (_data: MetadataUpdateResponse) => called++)

    await rabbit.deleteStream(streamName)

    await eventually(async () => expect(called).greaterThanOrEqual(1), 10000)
  }).timeout(10000)
})
