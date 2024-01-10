import { Client } from "../../src"
import { createClient } from "../support/fake_data"
import { expect } from "chai"
import { startSuperStream, stopSuperStream, username, password } from "../support/util"
import { randomUUID } from "node:crypto"

describe.only("RouteQuery command", () => {
  let client: Client
  let superStream = ""

  beforeEach(async () => {
    client = await createClient(username, password)
    superStream = `super-stream-test-${randomUUID()}`
  })

  afterEach(async () => {
    await client.close()
    await stopSuperStream(superStream)
  })

  it("returns a list of stream names", async () => {
    await startSuperStream(superStream)

    const route = await client.routeQuery({ routingKey: "0", superStream: superStream })

    expect(route).contains(`${superStream}-0`)
  }).timeout(10000)
})
