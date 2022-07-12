import { expect } from "chai"
import { connect, Connection } from "../../src"
import { Rabbit } from "../support/rabbit"

describe("Stream", () => {
  const rabbit = new Rabbit()
  const streamName = "test-stream"
  const payload = { "x-max-age": "test" }
  let connection: Connection

  before(async () => {
    connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })
  })

  afterEach(async () => await rabbit.deleteQueue("%2F", streamName))
  after(() => rabbit.closeAllConnections())

  describe("Create", () => {
    it("Should create a new Stream", async () => {
      const resp = await connection.createStream({
        stream: streamName,
        arguments: payload,
      })

      expect(resp.ok).to.be.true
      const result = await rabbit.getQueue("%2F", streamName)
      expect(result.name).to.be.eql(streamName)
    })

    it("Should detect a duplicate Stream", async () => {
      await connection.createStream({
        stream: streamName,
        arguments: payload,
      })

      const errorResp = await connection.createStream({
        stream: streamName,
        arguments: payload,
      })
      expect(errorResp.ok).to.be.false
    })
  })
})
