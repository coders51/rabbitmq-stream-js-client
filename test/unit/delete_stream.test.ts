import { connect, Connection } from "../../src"
import { expect } from "chai"
import { Rabbit } from "../support/rabbit"

describe("Delete command", () => {
  let rabbit: Rabbit
  let connection: Connection
  const queue_name = `queue_${(Math.random() * 10) | 0}`

  afterEach(async () => {
    await rabbit.closeAllConnections()
    await rabbit.deleteAllQueues()
  })

  beforeEach(async () => {
    rabbit = new Rabbit()
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

  it("delete a nonexisting stream", async () => {
    const result = await connection.deleteStream({ stream: "AAAAA" })
    expect(result.ok).to.be.false
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
    expect(result.ok).to.be.true
    expect(errorOnRetrieveAfterDeletion).is.not.null
  })
})
