import { inspect } from "util"
import { connect } from "../../src"
import { Offset } from "../../src/requests/subscribe_request"
import { Consumer } from "../../src/consumer"
import { Rabbit } from "../support/rabbit"

describe("consumer", () => {
  const rabbit = new Rabbit()
  const streamName = "test-stream"

  before(async () => {
    await rabbit.createStream(streamName)
  })

  after(async () => {
    await rabbit.deleteStream(streamName)
  })

  it("using parameters", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0, // not used
      heartbeat: 0, // not user
    })

    const consumer = Consumer.create({
      connection: connection,
      streamName: streamName,
      offset: Offset.next(),
      messageHandler: async (message) => {
        console.log(`received message: ${inspect(message)}`)
      },
    })

    await consumer.close()

    await connection.close()
  }).timeout(10000)

  it("raise exception if goes in timeout")
  it("raise exception if server refuse port")
})
