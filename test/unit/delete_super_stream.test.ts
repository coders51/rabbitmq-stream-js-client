import { expect } from "chai"
import { Client } from "../../src"
import { createClient } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { expectToThrowAsync, password, username } from "../support/util"
import { coerce, lt } from "semver"

describe("Delete Super Stream command", () => {
  const rabbit: Rabbit = new Rabbit(username, password)
  let client: Client
  const streamName = `stream_${(Math.random() * 10) | 0}`

  before(async function () {
    client = await createClient(username, password)
    // eslint-disable-next-line no-invalid-this
    if (lt(coerce(client.rabbitManagementVersion)!, "3.13.0")) this.skip()
  })

  afterEach(async () => {
    try {
      await rabbit.deleteAllQueues({ match: /stream_/ })
      await rabbit.deleteExchange(streamName)
    } catch (error) {}
  })

  after(async () => {
    try {
      await client.close()
      await rabbit.closeAllConnections()
    } catch (error) {}
  })

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
