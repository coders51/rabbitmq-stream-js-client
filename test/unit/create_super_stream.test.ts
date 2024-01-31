import { expect } from "chai"
import { Client } from "../../src"
import { randomUUID } from "crypto"
import { createClient } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { expectToThrowAsync, password, username } from "../support/util"
import { coerce, lt } from "semver"

describe("Super Stream", () => {
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

  before(async function () {
    client = await createClient(username, password)
    // eslint-disable-next-line no-invalid-this
    if (lt(coerce(client.rabbitManagementVersion)!, "3.13.0")) this.skip()
  })

  afterEach(async () => {
    try {
      await rabbit.deleteAllQueues({ match: /test-stream-/ })
      await rabbit.deleteExchange(streamName)
    } catch (error) {}
  })

  after(async () => {
    try {
      await client.close()
      await rabbit.closeAllConnections()
    } catch (error) {}
  })

  describe("Create", () => {
    it("Should create a new Super Stream with 3 partitions by default", async () => {
      const resp = await client.createSuperStream({ streamName, arguments: payload })

      expect(resp).to.be.true
      const result = await rabbit.getSuperStreamQueues("%2F", streamName)
      expect(result.map((r) => r.name)).to.have.members(Array.from(Array(3).keys()).map((n) => `${streamName}-${n}`))
    })

    it("Should create a new Super Stream with 3 partitions by default with the given arguments", async () => {
      const resp = await client.createSuperStream({ streamName, arguments: payload })

      expect(resp).to.be.true
      const result = await rabbit.getSuperStreamQueues("%2F", streamName)
      expect(result.map((r) => r.name)).to.have.members(Array.from(Array(3).keys()).map((n) => `${streamName}-${n}`))
      await Promise.all(
        Array.from(Array(3).keys()).map(async (n) => {
          const queue = await rabbit.getQueueInfo(`${streamName}-${n}`)
          expect(queue.arguments).to.be.eql({
            "x-queue-type": "stream",
            "x-queue-leader-locator": payload["queue-leader-locator"],
            "x-max-age": payload["max-age"],
            "x-stream-max-segment-size-bytes": payload["stream-max-segment-size-bytes"],
            "x-initial-cluster-size": payload["initial-cluster-size"],
            "x-max-length-bytes": payload["max-length-bytes"],
          })
        })
      )
    })

    it("Should create a new Super Stream with 2 partitions", async () => {
      const resp = await client.createSuperStream({ streamName, arguments: payload }, undefined, 2)

      expect(resp).to.be.true
      const result = await rabbit.getSuperStreamQueues("%2F", streamName, 2)
      expect(result.map((r) => r.name)).to.have.members(Array.from(Array(2).keys()).map((n) => `${streamName}-${n}`))
    })

    it("Should create a new Super Stream with 2 partitions and with bindingKeys", async () => {
      const resp = await client.createSuperStream({ streamName, arguments: payload }, ["A", "B"], 2)

      expect(resp).to.be.true
      const result = await rabbit.getSuperStreamQueues("%2F", streamName, 2, ["A", "B"])
      expect(result.map((r) => r.name)).to.have.members(["A", "B"].map((bk) => `${streamName}-${bk}`))
    })

    it("Should be idempotent and ignore a duplicate Stream error", async () => {
      await client.createSuperStream({ streamName, arguments: payload })
      const resp = await client.createSuperStream({ streamName, arguments: payload })

      expect(resp).to.be.true
    })

    it("Should raise an error if creation goes wrong", async () => {
      await expectToThrowAsync(
        () => client.createSuperStream({ streamName: "", arguments: payload }),
        Error,
        "Create Super Stream command returned error with code 17"
      )
    })
  })
})
