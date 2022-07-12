import { connect } from "../../src"
import { Rabbit } from "../support/rabbit"

describe("declare publisher", () => {
  const rabbit = new Rabbit()
  const testStreamName = "test-stream"

  beforeEach(async () => {
    await rabbit.createStream(testStreamName)
  })

  afterEach(async () => {
    await rabbit.deleteStream(testStreamName)
  })

  it("declaring a publisher on an existing stream - the publisher should be created", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0, // not used
      heartbeat: 0, // not user
    })

    await connection.declarePublisher({ stream: testStreamName })

  })

  it("declaring a publisher on a non-existing stream should raise an error", async () => {})
})
