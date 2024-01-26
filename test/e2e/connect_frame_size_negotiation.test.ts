import { expect } from "chai"
import { createClient } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"

describe("connect frame size negotiation", () => {
  const rabbit = new Rabbit(username, password)

  it("using 65536 as frameMax", async () => {
    const frameMax = 65536

    const client = await createClient(username, password, undefined, frameMax)

    await eventually(async () => {
      expect(client.maxFrameSize).lte(frameMax)
      expect(await rabbit.getConnections()).lengthOf(1)
    }, 5000)
    try {
      await client.close()
      await rabbit.closeAllConnections()
    } catch (e) {}
  }).timeout(10000)

  it("using 1024 as frameMax", async () => {
    const frameMax = 1024

    const client = await createClient(username, password, undefined, frameMax)

    await eventually(async () => {
      expect(client.maxFrameSize).lte(frameMax)
      expect(await rabbit.getConnections()).lengthOf(1)
    }, 5000)
    try {
      await client.close()
      await rabbit.closeAllConnections()
    } catch (e) {}
  }).timeout(10000)
})
