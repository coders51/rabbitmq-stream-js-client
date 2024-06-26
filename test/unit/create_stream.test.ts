import { expect } from "chai"
import { randomUUID } from "crypto"
import { Client } from "../../src"
import { createClient } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { expectToThrowAsync, password, username } from "../support/util"

describe("Stream", () => {
  const rabbit = new Rabbit(username, password)
  const streamName = `test-stream-${randomUUID()}`
  const payload = {
    "queue-leader-locator": "random" as const,
    "max-age": "120s",
    "stream-max-segment-size-bytes": 1000,
    "initial-cluster-size": 5,
    "max-length-bytes": 20000,
  }
  let client: Client

  beforeEach(async () => {
    client = await createClient(username, password)
  })

  afterEach(async () => {
    try {
      await rabbit.deleteQueue("%2F", streamName)
    } catch (error) {}
  })
  afterEach(async () => {
    try {
      await client.close()
    } catch (error) {}
  })

  after(() => rabbit.closeAllConnections())

  describe("Create", () => {
    it("Should create a new Stream", async () => {
      const resp = await client.createStream({ stream: streamName, arguments: payload })

      expect(resp).to.be.true
      const result = await rabbit.getQueue("%2F", streamName)
      expect(result.name).to.be.eql(streamName)
    })

    it("Should create a new Stream with the given arguments", async () => {
      const resp = await client.createStream({ stream: streamName, arguments: payload })

      expect(resp).to.be.true
      const result = await rabbit.getQueueInfo(streamName)
      expect(result.arguments).to.be.eql({
        "x-queue-type": "stream",
        "x-queue-leader-locator": payload["queue-leader-locator"],
        "x-max-age": payload["max-age"],
        "x-stream-max-segment-size-bytes": payload["stream-max-segment-size-bytes"],
        "x-initial-cluster-size": payload["initial-cluster-size"],
        "x-max-length-bytes": payload["max-length-bytes"],
      })
    })

    it("Should be idempotent and ignore a duplicate Stream error", async () => {
      await client.createStream({ stream: streamName, arguments: payload })
      const resp = await client.createStream({ stream: streamName, arguments: payload })

      expect(resp).to.be.true
    })

    it("Should raise an error if creation goes wrong", async () => {
      await expectToThrowAsync(
        () => client.createStream({ stream: "", arguments: payload }),
        Error,
        "Create Stream command returned error with code 17"
      )
    })
  })
})
