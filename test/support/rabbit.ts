import got from "got"

interface RabbitConnectionResponse {
  name: string
}

export class Rabbit {
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
