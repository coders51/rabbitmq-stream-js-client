import { Connection } from "../../src"
import { expect } from "chai"
import { Rabbit } from "../support/rabbit"
import { randomUUID } from "crypto"
import { expectToThrowAsync, username, password } from "../support/util"
import { createConnection } from "../support/fake_data"

describe("StreamStats", () => {
  const rabbit = new Rabbit(username, password)
  const testStreamName = "test-stream"
  let connection: Connection
  let publisherRef: string

  beforeEach(async () => {
    publisherRef = randomUUID()
    await rabbit.createStream(testStreamName)
    connection = await createConnection(username, password)
  })

  afterEach(async () => {
    await connection.close()
    await rabbit.deleteStream(testStreamName)
  })

  it("gets statistics for a stream", async () => {
    const publisher = await connection.declarePublisher({ stream: testStreamName, publisherRef })
    for (let i = 0; i < 5; i++) {
      await publisher.send(Buffer.from(`test${randomUUID()}`))
    }

    const stats = await connection.streamStatsRequest(testStreamName)

    expect(stats.committedChunkId).to.be.a("BigInt")
    expect(stats.firstChunkId).to.be.a("BigInt")
    expect(stats.lastChunkId).to.be.a("BigInt")
  }).timeout(10000)

  it("returns an error when the stream does not exist", async () => {
    await expectToThrowAsync(
      () => connection.streamStatsRequest("stream-does-not-exist"),
      Error,
      "Stream Stats command returned error with code 2 - Stream does not exist"
    )
  }).timeout(10000)
})
