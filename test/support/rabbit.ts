import got from "got"

interface RabbitConnectionResponse {
  name: string
}

interface RabbitPublishersResponse {
  reference: string
  publisher_id: number
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

  async createStream(streamName: string): Promise<void> {
    await got.put<unknown>(`http://localhost:15672/api/queues/%2F/${streamName}`, {
      body: JSON.stringify({ auto_delete: false, durable: true, arguments: { "x-queue-type": "stream" } }),
      username: "rabbit",
      password: "rabbit",
      responseType: "json",
    })
    return
  }

  async deleteStream(streamName: string): Promise<void> {
    await got.delete<unknown>(`http://localhost:15672/api/queues/%2F/${streamName}`, {
      username: "rabbit",
      password: "rabbit",
    })
    return
  }

  async returnPublishers(streamName: string): Promise<string[]> {
    const publishers = await got
      .get<RabbitPublishersResponse[]>(`http://localhost:15672/api/stream/publishers/%2F/${streamName}`, {
        username: "rabbit",
        password: "rabbit",
        responseType: "json",
      })
      .then((resp) => {
        return resp.body.map((p) => p.reference)
      })
    return publishers
  }
}
