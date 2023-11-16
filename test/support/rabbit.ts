import got from "got"
interface RabbitConnectionResponse {
  name: string
}

interface RabbitConsumerCredits {
  connectionName: string
  allCredits: number[]
}

// not completed
interface MessageInfoResponse {
  messages: number
  messages_ready: number
  messages_unacknowledged: number
  types: "stream" | "quorum" | "classic"
}

interface RabbitPublishersResponse {
  reference: string
  publisher_id: number
}

interface RabbitConsumersResponseQueue {
  name: string
  vhost: string
}

interface RabbitChannelDetails {
  connection_name: string
  name: string
  node: string
  number: number
  peer_host: string
  peer_port: number
  user: string
}

interface RabbitConsumersResponse {
  queue: RabbitConsumersResponseQueue
  consumer_tag: string
  channel_details: RabbitChannelDetails
}

interface RabbitConnectionDetails {
  credits: number
}

interface RabbitQueueResponse {
  arguments: Record<string, string>
  auto_delete: boolean
  durable: boolean
  exclusive: boolean
  name: string
  node: string
  type: string
  vhost: string
}

export class Rabbit {
  constructor(
    private username: string,
    private password: string,
  ) {}

  async closeAllConnections(): Promise<void> {
    const l = await this.getConnections()
    await Promise.all(l.map((c) => this.closeConnection(c.name)))
  }

  async closeConnection(name: string) {
    return got.delete(`http://localhost:15672/api/connections/${name}`, {
      username: this.username,
      password: this.password,
      responseType: "json",
    })
  }

  async getQueueInfo(queue: string): Promise<MessageInfoResponse> {
    const ret = await got.get<MessageInfoResponse>(`http://localhost:15672/api/queues/%2F/${queue}`, {
      username: this.username,
      password: this.password,
      responseType: "json",
    })
    return ret.body
  }

  async getMessages(queue: string) {
    // I think it's not possible to execute on stream queue
    const ret = await got.post<unknown>(`http://localhost:15672/api/queues/%2F/${queue}/get`, {
      username: this.username,
      password: this.password,
      responseType: "json",
      body: JSON.stringify({ count: 100, ackmode: "ack_requeue_false", encoding: "auto", truncate: 50000 }),
    })
    return ret.body
  }

  async getConnections(): Promise<RabbitConnectionResponse[]> {
    const ret = await got.get<RabbitConnectionResponse[]>(`http://localhost:15672/api/connections`, {
      username: this.username,
      password: this.password,
      responseType: "json",
    })
    return ret.body
  }

  createStream(streamName: string) {
    return got.put<unknown>(`http://localhost:15672/api/queues/%2F/${streamName}`, {
      body: JSON.stringify({ auto_delete: false, durable: true, arguments: { "x-queue-type": "stream" } }),
      username: this.username,
      password: this.password,
      responseType: "json",
    })
  }

  deleteStream(streamName: string) {
    return got.delete<unknown>(`http://localhost:15672/api/queues/%2F/${streamName}`, {
      username: this.username,
      password: this.password,
    })
  }

  async returnPublishers(streamName: string): Promise<string[]> {
    const resp = await got.get<RabbitPublishersResponse[]>(
      `http://localhost:15672/api/stream/publishers/%2F/${streamName}`,
      {
        username: this.username,
        password: this.password,
        responseType: "json",
      },
    )
    return resp.body.map((p) => p.reference)
  }

  async returnConsumers(): Promise<string[]> {
    const resp = await got.get<RabbitConsumersResponse[]>(`http://localhost:15672/api/consumers/%2F/`, {
      username: this.username,
      password: this.password,
      responseType: "json",
    })
    return resp.body.map((p) => p.consumer_tag)
  }

  async returnConsumersCredits(): Promise<RabbitConsumerCredits[]> {
    const allConsumerCredits: RabbitConsumerCredits[] = []
    const allConsumersResp = await got.get<RabbitConsumersResponse[]>(`http://localhost:15672/api/consumers`, {
      username: "rabbit",
      password: "rabbit",
      responseType: "json",
    })
    const consumerChannelDetails = allConsumersResp.body.map((d) => d.channel_details)
    for (const consumerChannelDetail of consumerChannelDetails) {
      const connectionName = consumerChannelDetail.connection_name
      const resp = await got.get<RabbitConnectionDetails[]>(
        `http://localhost:15672/api/stream/connections/%2F/${connectionName}/consumers`,
        {
          username: "rabbit",
          password: "rabbit",
          responseType: "json",
        },
      )
      allConsumerCredits.push({ connectionName, allCredits: resp.body.map((rcd) => rcd.credits) })
    }
    return allConsumerCredits
  }

  async getQueue(vhost: string = "%2F", name: string): Promise<RabbitQueueResponse> {
    const ret = await got.get<RabbitQueueResponse>(`http://localhost:15672/api/queues/${vhost}/${name}`, {
      username: this.username,
      password: this.password,
      responseType: "json",
    })
    return ret.body
  }

  async deleteQueue(vhost: string = "%2F", name: string): Promise<void> {
    await got.delete(`http://localhost:15672/api/queues/${vhost}/${name}`, {
      username: this.username,
      password: this.password,
      responseType: "json",
    })
  }

  async createQueue(vhost: string = "%2F", name: string): Promise<RabbitConnectionResponse> {
    const r = await got.put<RabbitConnectionResponse>(`http://localhost:15672/api/queues/${vhost}/${name}`, {
      json: { arguments: { "x-queue-type": "stream" }, durable: true },
      username: this.username,
      password: this.password,
    })

    return r.body
  }

  async getQueues(): Promise<RabbitQueueResponse[]> {
    const ret = await got.get<RabbitQueueResponse[]>(`http://localhost:15672/api/queues`, {
      username: this.username,
      password: this.password,
      responseType: "json",
    })
    return ret.body
  }

  async deleteAllQueues({ match }: { match: RegExp } = { match: /.*/ }): Promise<void> {
    const l = await this.getQueues()
    await Promise.all(l.filter((q) => match && q.name.match(match)).map((q) => this.deleteQueue("%2F", q.name)))
  }
}
