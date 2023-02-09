import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect, Connection } from "../../src"
import { MetadataUpdateResponse } from "../../src/responses/metadata_update_response"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"

describe("update the metadata from the server", () => {
  const rabbit = new Rabbit()
  let connection: Connection
  const metadataUpdateResponses: MetadataUpdateResponse[] = []

  beforeEach(async () => {
    metadataUpdateResponses.length = 0
    connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0, // not used
      heartbeat: 0, // not used
      listeners: {
        metadata_update: (data) => metadataUpdateResponses.push(data),
      },
    })
  })
  afterEach(() => connection.close())

  it("when delete stream we receive a metadataUpdate", async () => {
    const stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    await connection.declarePublisher({ stream, publisherRef: "my publisher" })

    await rabbit.deleteStream(stream)

    await eventually(async () => expect(metadataUpdateResponses.length).greaterThanOrEqual(1), 10000)
  }).timeout(10000)

  it("when delete stream we receive a metadataUpdate registering after creation", async () => {
    const stream = `my-stream-${randomUUID()}`
    let called = 0
    await rabbit.createStream(stream)
    await connection.declarePublisher({ stream, publisherRef: "my publisher" })
    connection.on("metadata_update", (_data: MetadataUpdateResponse) => called++)

    await rabbit.deleteStream(stream)

    await eventually(async () => expect(called).greaterThanOrEqual(1), 10000)
  }).timeout(10000)
})
