import { expect } from "chai"
import { Client } from "../../src"
import { createClient } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"

describe("connect", () => {
  let client: Client
  const rabbit = new Rabbit(username, password)

  beforeEach(async () => {
    client = await createClient(username, password)
  })

  afterEach(async () => {
    try {
      await client.close()
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
