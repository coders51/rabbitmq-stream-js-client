import { Connection } from "../../src"
import { createConnection } from "../support/fake_data"
import { expect } from "chai"
import { startSuperStream, stopSuperStream, username, password } from "../support/util"

describe("RouteQuery command", () => {
  let connection: Connection
  const superStream = "superStream"

  beforeEach(async () => {
    connection = await createConnection(username, password)
  })

  afterEach(async () => {
    await connection.close()
    await stopSuperStream(superStream)
  })

  it("is true", async () => {
    await startSuperStream(superStream)
    const route = await connection.routeQuery({ routingKey: "0", superStream: superStream })
    expect(route).contains("superStream-0")
  }).timeout(10000)
})
