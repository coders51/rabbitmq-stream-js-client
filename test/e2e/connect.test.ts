import { expect } from "chai"
import { connect } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"

describe("connect", () => {
  const rabbit = new Rabbit()

  afterEach(() => rabbit.closeAllConnections())

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

    await eventually(async () => {
      expect(await rabbit.getConnections()).lengthOf(1)
    }, 5000)
    await connection.close()
  }).timeout(10000)

  it("raise exception if goes in timeout")
  it("raise exception if server refuse port")
})
