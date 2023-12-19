import { expect } from "chai"
import { createConnection } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"

describe("connect frame size negotiation", () => {
  const rabbit = new Rabbit(username, password)

  it("using 65536 as frameMax", async () => {
    const frameMax = 65536

    const connection = await createConnection(username, password, undefined, frameMax)

    await eventually(async () => {
      expect(connection.currentFrameMax).lte(frameMax)
      expect(await rabbit.getConnections()).lengthOf(1)
    }, 5000)
    try {
      await connection.close()
      await rabbit.closeAllConnections()
    } catch (e) {}
  }).timeout(10000)

  it("using 1024 as frameMax", async () => {
    const frameMax = 1024

    const connection = await createConnection(username, password, undefined, frameMax)

    await eventually(async () => {
      expect(connection.currentFrameMax).lte(frameMax)
      expect(await rabbit.getConnections()).lengthOf(1)
    }, 5000)
    try {
      await connection.close()
      await rabbit.closeAllConnections()
    } catch (e) {}
  }).timeout(10000)
})
