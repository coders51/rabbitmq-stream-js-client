import { expect } from "chai"
import { Client } from "../../src"
import { createClient } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"

describe("connect", () => {
  let client: Client
  const rabbit = new Rabbit(username, password)

  afterEach(async () => {
    try {
      await client.close()
    } catch (e) {}

    try {
      await rabbit.closeAllConnections()
    } catch (e) {}
  })

  it("using parameters", async () => {
    client = await createClient(username, password)

    await eventually(async () => {
      expect(await rabbit.getConnections()).lengthOf(1)
    }, 5000)
  }).timeout(10000)

  // TODO -> Need a way to test connection phase timeout and not inactivity timeout
  // it("raise exception if goes in timeout", async () => {
  //   client = await createClient(username, password)

  //   await expectToThrowAsync(async () => await wait(10000), Error, "Timeout rabbitmq:5552")
  // }).timeout(15000)

  it("raise exception if server refuse port", async () => {
    createClient(username, password, undefined, undefined, undefined, 5550).catch((err) => {
      expect(err).to.not.be.null
    })
  }).timeout(10000)
})
