import { expect } from "chai"
import { connect } from "../../src"
import { Rabbit } from "../support/rabbit"

describe("Stream", () => {
  const rabbit = new Rabbit()
  const streamName = "test-stream"
  const payload = { key: "x-dead-letter-exchange", value: "test" }

  afterEach(async () => await rabbit.deleteQueue("%2F", streamName))

  describe("Create", () => {
    it("Should create a new Stream", async () => {
      const connection = await connect({
        hostname: "localhost",
        port: 5552,
        username: "rabbit",
        password: "rabbit",
        vhost: "/",
        frameMax: 0,
        heartbeat: 0,
      })

      const resp = await connection.createStream({
        stream: streamName,
        arguments: payload,
      })

      expect(resp.ok).to.be.true
      const result = await rabbit.getQueue("%2F", streamName)
      expect(result.name).to.be.eql(streamName)
    })

    it("Should detect a duplicate Stream", async () => {
      const connection = await connect({
        hostname: "localhost",
        port: 5552,
        username: "rabbit",
        password: "rabbit",
        vhost: "/",
        frameMax: 0,
        heartbeat: 0,
      })

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

    it("Should ignore invalid arguments", async () => {
      const connection = await connect({
        hostname: "localhost",
        port: 5552,
        username: "rabbit",
        password: "rabbit",
        vhost: "/",
        frameMax: 0,
        heartbeat: 0,
      })

      await connection.createStream({
        stream: streamName,
        arguments: { key: "fake-argument", value: "test" },
      })

      const result = await rabbit.getQueue("%2F", streamName)
      expect(result.arguments.keys.includes("fake-argument")).to.be.false
    })
  })
})
