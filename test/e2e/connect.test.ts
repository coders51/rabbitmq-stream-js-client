import { expect } from "chai"
import got from "got"
import { connect } from "../../src"
import { eventually } from "../support/util"

interface RabbitConnectionResponse {
  name: string
}

class Rabbit {
  async closeAllConnections(): Promise<void> {
    const l = await this.getConnections()
    await Promise.all(l.map((c) => this.closeConnection(c)))
  }

  async closeConnection(c: RabbitConnectionResponse): Promise<void> {
    const x = await got.delete(`http://localhost:15672/api/connections/${c.name}`, {
      username: "rabbit",
      password: "rabbit",
      responseType: "json",
    })
    console.log(x.body)
  }
  async getConnections(): Promise<RabbitConnectionResponse[]> {
    const ret = await got.get<RabbitConnectionResponse[]>(`http://localhost:15672/api/connections`, {
      username: "rabbit",
      password: "rabbit",
      responseType: "json",
    })
    return ret.body
  }
}

describe("connect", () => {
  const rabbit = new Rabbit()

  afterEach(() => rabbit.closeAllConnections())

  it("using parameters", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0, // not used
      heartbeat: 0, // not user
    })

    await eventually(async () => {
      expect(await rabbit.getConnections()).lengthOf(1)
    }, 5000)
    await connection.close()
  }).timeout(10000)

  it("raise exception if goes in timeout")
  it("raise exception if server refuse port")
})
