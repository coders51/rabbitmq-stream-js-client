import { expect } from "chai"
import { Connection, ListenersParams } from "../../src"
import { MetadataUpdateResponse } from "../../src/responses/metadata_update_response"
import { createConnection, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"

describe("update the metadata from the server", () => {
  const rabbit = new Rabbit("rabbit", "rabbit")
  let connection: Connection
  let streamName: string
  const metadataUpdateResponses: MetadataUpdateResponse[] = []

  beforeEach(async () => {
    metadataUpdateResponses.length = 0
    const listeners: ListenersParams = {
      metadata_update: (data) => metadataUpdateResponses.push(data),
    }
    connection = await createConnection(listeners)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    try {
      await connection.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("when delete stream we receive a metadataUpdate", async () => {
    await createPublisher(streamName, connection)
    await rabbit.deleteStream(streamName)
    await eventually(async () => expect(metadataUpdateResponses.length).greaterThanOrEqual(1), 10000)
  }).timeout(10000)

  it("when delete stream we receive a metadataUpdate registering after creation", async () => {
    let called = 0
    await createPublisher(streamName, connection)
    connection.on("metadata_update", (_data: MetadataUpdateResponse) => called++)

    await rabbit.deleteStream(streamName)

    await eventually(async () => expect(called).greaterThanOrEqual(1), 10000)
  }).timeout(10000)
})
