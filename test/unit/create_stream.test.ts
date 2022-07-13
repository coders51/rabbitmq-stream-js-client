import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect, Connection } from "../../src"
import { Rabbit } from "../support/rabbit"
import { expectToThrowAsync } from "../support/util"

describe("Stream", () => {
  const rabbit = new Rabbit()
  const streamName = `test-stream-${randomUUID()}`
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

  afterEach(async () => {
    try {
      await rabbit.deleteQueue("%2F", streamName)
    } catch (error) {}
  })
  after(() => rabbit.closeAllConnections())

  describe("Create", () => {
    it("Should create a new Stream", async () => {
      const resp = await connection.createStream({
        stream: streamName,
        arguments: payload,
      })

      expect(resp).to.be.true
      const result = await rabbit.getQueue("%2F", streamName)
      expect(result.name).to.be.eql(streamName)
    })

    it("Should detect a duplicate Stream", async () => {
      await connection.createStream({
        stream: streamName,
        arguments: payload,
      })

      await expectToThrowAsync(
        () =>
          connection.createStream({
            stream: streamName,
            arguments: payload,
          }),
        Error,
        "Create Stream command returned error with code 5"
      )
    })

    it("Should raise an error if creation goes wrong", async () => {
      await expectToThrowAsync(
        () => connection.createStream({ stream: "", arguments: payload }),
        Error,
        "Create Stream command returned error with code 17"
      )
    })
  })
})
