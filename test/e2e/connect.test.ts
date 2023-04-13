import { expect } from "chai"
import { Connection } from "../../src"
import { createConnection } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"

describe("connect", () => {
  let connection: Connection
  const rabbit = new Rabbit("rabbit", "rabbit")

  beforeEach(async () => {
    connection = await createConnection()
  })

  afterEach(async () => {
    try {
      await connection.close()
    } catch (e) {}

    try {
      await rabbit.closeAllConnections()
    } catch (e) {}
  })

  it("using parameters", async () => {
    await eventually(async () => {
      expect(await rabbit.getConnections()).lengthOf(1)
    }, 5000)
  }).timeout(10000)

  it("raise exception if goes in timeout")
  it("raise exception if server refuse port")
})
