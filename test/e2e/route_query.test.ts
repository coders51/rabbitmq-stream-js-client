import { Client } from "../../src"
import { createClient } from "../support/fake_data"
import { expect } from "chai"
import { username, password, maybeStopSuperStream, maybeStartSuperStream } from "../support/util"

describe("RouteQuery command", () => {
  let client: Client
  const superStream = `super-stream-test`

  beforeEach(async () => {
    client = await createClient(username, password)
  })

  afterEach(async () => {
    await client.close()
    await maybeStopSuperStream(superStream)
  })

  it("returns a list of stream names", async () => {
    await maybeStartSuperStream(superStream)

    const route = await client.routeQuery({ routingKey: "0", superStream: superStream })

    expect(route).contains(`${superStream}-0`)
  }).timeout(10000)
})
