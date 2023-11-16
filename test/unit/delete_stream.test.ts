import { expect } from "chai"
import { Connection } from "../../src"
import { createConnection } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { expectToThrowAsync, password, username } from "../support/util"

describe("Delete command", () => {
  const rabbit: Rabbit = new Rabbit(username, password)
  let connection: Connection
  const queue_name = `queue_${(Math.random() * 10) | 0}`

  beforeEach(async () => {
    connection = await createConnection(username, password)
  })

  afterEach(async () => {
    await rabbit.deleteAllQueues({ match: /queue/ })
  })

  afterEach(async () => {
    try {
      await connection.close()
    } catch (error) {}
  })

  after(() => rabbit.closeAllConnections())

  it("delete a nonexisting stream (raises error)", async () => {
    await expectToThrowAsync(
      () => connection?.deleteStream({ stream: "AAA" }),
      Error,
      "Delete Stream command returned error with code 2"
    )
  })

  it("delete an existing stream", async () => {
    await rabbit.createQueue("%2F", queue_name)
    await rabbit.getQueue("%2F", queue_name)
    let errorOnRetrieveAfterDeletion = null

    const result = await connection?.deleteStream({ stream: queue_name })

    try {
      await rabbit.getQueue("%2F", queue_name)
    } catch (e) {
      errorOnRetrieveAfterDeletion = e
    }
    expect(result).to.be.true
    expect(errorOnRetrieveAfterDeletion).is.not.null
  })
})
