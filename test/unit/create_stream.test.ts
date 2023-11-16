import { expect } from "chai"
import { randomUUID } from "crypto"
import { Connection } from "../../src"
import { createConnection } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { expectToThrowAsync, password, username } from "../support/util"

describe("Stream", () => {
  const rabbit = new Rabbit(username, password)
  const streamName = `test-stream-${randomUUID()}`
  const payload = {
    "x-queue-leader-locator": "test",
    "x-max-age": "test",
    "x-stream-max-segment-size-bytes": 42,
    "x-initial-cluster-size": 42,
    "x-max-length-bytes": 42,
  }
  let connection: Connection

  beforeEach(async () => {
    connection = await createConnection(username, password)
  })

  afterEach(async () => {
    try {
      await rabbit.deleteQueue("%2F", streamName)
    } catch (error) {}
  })
  afterEach(async () => {
    try {
      await connection.close()
    } catch (error) {}
  })

  after(() => rabbit.closeAllConnections())

  describe("Create", () => {
    it("Should create a new Stream", async () => {
      const resp = await connection.createStream({ stream: streamName, arguments: payload })

      expect(resp).to.be.true
      const result = await rabbit.getQueue("%2F", streamName)
      expect(result.name).to.be.eql(streamName)
    })

    it("Should be idempotent and ignore a duplicate Stream error", async () => {
      await connection.createStream({ stream: streamName, arguments: payload })
      const resp = await connection.createStream({ stream: streamName, arguments: payload })

      expect(resp).to.be.true
    })

    it("Should raise an error if creation goes wrong", async () => {
      await expectToThrowAsync(
        () => connection.createStream({ stream: "", arguments: payload }),
        Error,
        "Create Stream command returned error with code 17",
      )
    })
  })
})
