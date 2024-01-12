import { Client } from "../../src"
import { createClient } from "../support/fake_data"
import { expect } from "chai"
import { username, password } from "../support/util"
import { Rabbit } from "../support/rabbit"

describe("PartitionsQuery command", () => {
  let client: Client
  const superStream = "super-stream-test"
  const rabbit = new Rabbit(username, password)

  beforeEach(async () => {
    client = await createClient(username, password)
  })

  afterEach(async () => {
    await client.close()
    await rabbit.deleteSuperStream(superStream)
  })

  it("returns a list of stream names", async () => {
    await rabbit.createSuperStream(superStream)

    const route = await client.partitionsQuery({ superStream: superStream })

    expect(route).contains("super-stream-test-0")
  }).timeout(10000)
})
