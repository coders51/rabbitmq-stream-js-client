import { expect } from "chai"
import { Client } from "../../src"
import { createClient } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"
import { Version } from "../../src/versions"
import { randomUUID } from "node:crypto"

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

  it("declaring connection name", async () => {
    const connectionName = `connection-name-${randomUUID()}`
    client = await createClient(username, password, undefined, undefined, undefined, undefined, connectionName)

    await eventually(async () => {
      const connections = await rabbit.getConnections()
      expect(connections.length).eql(1)
      expect(connections[0].client_properties?.connection_name).eql(connectionName)
    }, 5000)
  }).timeout(10000)

  it("and receive server-side message version declarations during handshake", async () => {
    client = await createClient(username, password)

    await eventually(async () => {
      const serverVersions = client.serverVersions
      expect(serverVersions.length).gt(0)
      expect(serverVersions).satisfies((versions: Version[]) => versions.every((version) => version.minVersion >= 1))
    }, 5000)
  }).timeout(10000)

  it("raise exception if server refuse port", async () => {
    createClient(username, password, undefined, undefined, undefined, 5550).catch((err) => {
      expect(err).to.not.be.null
    })
  }).timeout(10000)
})
