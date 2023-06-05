import { Connection } from "../../src"
import { createConnection } from "../support/fake_data"
import { expect } from "chai"
import { startSuperStream, stopSuperStream, username, password } from "../support/util"

describe("RouteQuery command", () => {
  let connection: Connection
  const superStream = "super-stream-test"

  beforeEach(async () => {
    connection = await createConnection(username, password)
  })

  afterEach(async () => {
    await connection.close()
    await stopSuperStream(superStream)
  })

  it("returns a list of stream names", async () => {
    await startSuperStream(superStream)
    const route = await connection.routeQuery({ routingKey: "0", superStream: superStream })
    expect(route).contains("super-stream-test-0")
  }).timeout(10000)
})
