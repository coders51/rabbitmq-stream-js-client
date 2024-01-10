import { Client } from "../../src"
import { createClient } from "../support/fake_data"
import { expect } from "chai"
import { username, password, maybeStopSuperStream, maybeStartSuperStream, expectToThrowAsync } from "../support/util"
import { randomUUID } from "crypto"
import { Rabbit } from "../support/rabbit"

describe("RouteQuery command", () => {
  let client: Client
  const rabbit = new Rabbit(username, password)
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

  it("throws when the super stream does not exist", async () => {
    const nonExistingStream = randomUUID()

    expectToThrowAsync(() => client.routeQuery({ routingKey: "0", superStream: nonExistingStream }), Error)
  })

  it("throws when the stream is not a super stream", async () => {
    const streamName = randomUUID()
    await rabbit.createStream(streamName)

    expectToThrowAsync(() => client.routeQuery({ routingKey: "0", superStream: streamName }), Error)
  })
})
