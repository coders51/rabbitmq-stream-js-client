import { connect, Connection } from "../../src"
import { expect } from "chai"
import { Rabbit } from "../support/rabbit"
import { expectToThrowAsync } from "../support/util"

describe("Delete command", () => {
  const rabbit: Rabbit = new Rabbit()
  let connection: Connection
  const queue_name = `queue_${(Math.random() * 10) | 0}`

  before(async () => {
    connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0, // not used
      heartbeat: 0, // not user
    })
  })

  after(() => rabbit.closeAllConnections())

  afterEach(async () => {
    await rabbit.deleteAllQueues()
  })

  it("delete a nonexisting stream (raises error)", async () => {
    await expectToThrowAsync(
      () => connection?.deleteStream({ stream: "AAA" }),
      Error,
      "Delete Stream command returned error with code 2"
    )
  })

  it("delete an existing stream", async () => {
    const created = await rabbit.createQueue("%2F", queue_name)
    const retrievedBeforeDeletion = await rabbit.getQueue("%2F", queue_name)
    let errorOnRetrieveAfterDeletion = null

    const result = await connection?.deleteStream({ stream: queue_name })
    try {
      await rabbit.getQueue("%2F", queue_name)
    } catch (e) {
      errorOnRetrieveAfterDeletion = e
    }

    expect(created).is.not.null
    expect(retrievedBeforeDeletion.name).equals(queue_name)
    expect(result).to.be.true
    expect(errorOnRetrieveAfterDeletion).is.not.null
  })
})
