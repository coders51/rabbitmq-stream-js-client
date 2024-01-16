import { expect } from "chai"
import { Client } from "../../src"
import { createClient } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { expectToThrowAsync, password, username } from "../support/util"

describe("Delete Super Stream command", () => {
  const rabbit: Rabbit = new Rabbit(username, password)
  let client: Client
  const streamName = `stream_${(Math.random() * 10) | 0}`

  beforeEach(async () => {
    client = await createClient(username, password)
  })

  afterEach(async () => {
    try {
      await rabbit.deleteAllQueues({ match: /stream_/ })
      await rabbit.deleteExchange(streamName)
    } catch (error) {}
  })

  afterEach(async () => {
    try {
      await client.close()
    } catch (error) {}
  })

  after(() => rabbit.closeAllConnections())

  it("delete a nonexisting super stream (raises error)", async () => {
    await expectToThrowAsync(
      () => client.deleteSuperStream({ streamName: "AAA" }),
      Error,
      "Delete Super Stream command returned error with code 2"
    )
  })

  it("delete an existing stream", async () => {
    await client.createSuperStream({ streamName, arguments: {} })
    let errorOnRetrieveAfterDeletion = null

    const result = await client.deleteSuperStream({ streamName })

    try {
      await rabbit.getSuperStreamQueues("%2F", streamName)
    } catch (e) {
      errorOnRetrieveAfterDeletion = e
    }
    expect(result).to.be.true
    expect(errorOnRetrieveAfterDeletion).is.not.null
  })
})
