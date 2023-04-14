import { expect } from "chai"
import { Connection } from "../../src"
import { createConnection } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"

describe("connect", () => {
  let connection: Connection
  const rabbit = new Rabbit(username, password)

  beforeEach(async () => {
    connection = await createConnection(username, password)
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
