import got, { HTTPError } from "got"
import { getTestNodesFromEnv } from "./util"
import { range } from "../../src/util"
import { CreateStreamArguments } from "../../src/requests/create_stream_request"

export interface RabbitConnectionResponse {
  name: string
  client_properties?: {
    connection_name?: string
    copyright?: string
    information?: string
    platform?: string
    product?: string
    version?: string
  }
}

export interface RabbitConsumerCredits {
  connectionName: string
  allCredits: number[]
}

// not completed
interface MessageInfoResponse {
  messages: number
  messages_ready: number
  messages_unacknowledged: number
  types: "stream" | "quorum" | "classic"
  node: string
  arguments?: CreateStreamArguments
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
  private port = process.env.RABBIT_MQ_MANAGEMENT_PORT || 15672
  private firstNode = getTestNodesFromEnv().shift()!
  constructor(
    private username: string,
    private password: string
  ) {}

  async closeAllConnections(): Promise<void> {
    const l = await this.getConnections()
    await Promise.all(l.map((c) => this.closeConnection(c.name)))
  }

  async closeConnection(name: string) {
    return got.delete(`http://${this.firstNode.host}:${this.port}/api/connections/${name}`, {
      username: this.username,
      password: this.password,
      responseType: "json",
    })
  }

  async getQueueInfo(queue: string): Promise<MessageInfoResponse> {
    const ret = await got.get<MessageInfoResponse>(
      `http://${this.firstNode.host}:${this.port}/api/queues/%2F/${queue}`,
      {
        username: this.username,
        password: this.password,
        responseType: "json",
      }
    )

    return ret.body
  }

  async getConnections(): Promise<RabbitConnectionResponse[]> {
    const ret = await got.get<RabbitConnectionResponse[]>(
      `http://${this.firstNode.host}:${this.port}/api/connections`,
      {
        username: this.username,
        password: this.password,
        responseType: "json",
      }
    )
    return ret.body
  }

  createStream(streamName: string) {
    return got.put<unknown>(`http://${this.firstNode.host}:${this.port}/api/queues/%2F/${streamName}`, {
      body: JSON.stringify({ auto_delete: false, durable: true, arguments: { "x-queue-type": "stream" } }),
      username: this.username,
      password: this.password,
      responseType: "json",
    })
  }

  async deleteStream(streamName: string) {
    try {
      const res = await got.delete<unknown>(`http://${this.firstNode.host}:${this.port}/api/queues/%2F/${streamName}`, {
        username: this.username,
        password: this.password,
      })
      return res
    } catch (e) {
      if (e instanceof HTTPError) {
        if (e.message === "Response code 404 (Not Found)") return ""
      }
      throw e
    }
  }

  createExchange(exchangeName: string) {
    return got.put<unknown>(`http://${this.firstNode.host}:${this.port}/api/exchanges/%2F/${exchangeName}`, {
      body: JSON.stringify({
        type: "direct",
        auto_delete: false,
        durable: true,
        arguments: { "x-super-stream-enabled": "true" },
      }),
      username: this.username,
      password: this.password,
      responseType: "json",
    })
  }

  deleteExchange(exchangeName: string) {
    return got.delete<unknown>(`http://${this.firstNode.host}:${this.port}/api/exchanges/%2F/${exchangeName}`, {
      username: this.username,
      password: this.password,
    })
  }

  createBinding(exchange: string, stream: string, routingKey: string) {
    return got.post<unknown>(`http://${this.firstNode.host}:${this.port}/api/bindings/%2F/e/${exchange}/q/${stream}`, {
      body: JSON.stringify({ routing_key: routingKey, arguments: { "x-stream-partition-order": routingKey } }),
      username: this.username,
      password: this.password,
    })
  }

  async createSuperStream(superStream: string, noOfPartitions = 3): Promise<number> {
    await this.deleteSuperStream(superStream)
    const exchangeName = `${superStream}`
    const streamNames = range(noOfPartitions).map((i) => `${superStream}-${i}`)
    await this.createExchange(exchangeName)
    await Promise.all(streamNames.map((sn) => this.createStream(sn)))
    await Promise.all(streamNames.map((sn, i) => this.createBinding(exchangeName, sn, `${i}`)))
    return noOfPartitions
  }

  async deleteSuperStream(superStream: string, noOfPartitions = 3, bindingKeys?: string[]) {
    try {
      await this.deleteExchange(superStream)
      const streamNames = bindingKeys
        ? bindingKeys.map((bk) => `${superStream}-${bk}`)
        : range(noOfPartitions).map((i) => `${superStream}-${i}`)
      await Promise.all(streamNames.map((sn) => this.deleteStream(sn)))
    } catch (e) {
      if (!(e && typeof e === "object" && "message" in e && /Response code 404/.test(e.message as string))) {
        throw e
      }
    }
  }

  async returnPublishers(streamName: string): Promise<string[]> {
    const resp = await got.get<RabbitPublishersResponse[]>(
      `http://${this.firstNode.host}:${this.port}/api/stream/publishers/%2F/${streamName}`,
      {
        username: this.username,
        password: this.password,
        responseType: "json",
      }
    )
    return resp.body.map((p) => p.reference)
  }

  async returnConsumers(): Promise<string[]> {
    const resp = await got.get<RabbitConsumersResponse[]>(
      `http://${this.firstNode.host}:${this.port}/api/consumers/%2F/`,
      {
        username: this.username,
        password: this.password,
        responseType: "json",
      }
    )
    return resp.body.map((p) => p.consumer_tag)
  }

  async returnConsumersCredits(): Promise<RabbitConsumerCredits[]> {
    const allConsumerCredits: RabbitConsumerCredits[] = []
    const allConsumersResp = await got.get<RabbitConsumersResponse[]>(
      `http://${this.firstNode.host}:${this.port}/api/consumers`,
      {
        username: "rabbit",
        password: "rabbit",
        responseType: "json",
      }
    )
    const consumerChannelDetails = allConsumersResp.body.map((d) => d.channel_details)
    for (const consumerChannelDetail of consumerChannelDetails) {
      const connectionName = consumerChannelDetail.connection_name
      const resp = await got.get<RabbitConnectionDetails[]>(
        `http://${this.firstNode.host}:${this.port}/api/stream/connections/%2F/${connectionName}/consumers`,
        {
          username: "rabbit",
          password: "rabbit",
          responseType: "json",
        }
      )
      allConsumerCredits.push({ connectionName, allCredits: resp.body.map((rcd) => rcd.credits) })
    }
    return allConsumerCredits
  }

  async getQueue(vhost: string = "%2F", name: string): Promise<RabbitQueueResponse> {
    const ret = await got.get<RabbitQueueResponse>(
      `http://${this.firstNode.host}:${this.port}/api/queues/${vhost}/${name}`,
      {
        username: this.username,
        password: this.password,
        responseType: "json",
      }
    )
    return ret.body
  }

  async getSuperStreamQueues(
    vhost: string = "%2F",
    name: string,
    numberOfPartitions: number = 3,
    bindingKeys?: string[]
  ): Promise<RabbitQueueResponse[]> {
    const streamNames = bindingKeys
      ? bindingKeys.map((bk) => `${name}-${bk}`)
      : range(numberOfPartitions).map((i) => `${name}-${i}`)
    return Promise.all(streamNames.map(async (sn) => await this.getQueue(vhost, sn)))
  }

  async deleteQueue(vhost: string = "%2F", name: string): Promise<void> {
    await got.delete(`http://${this.firstNode.host}:${this.port}/api/queues/${vhost}/${name}`, {
      username: this.username,
      password: this.password,
      responseType: "json",
    })
  }

  async createQueue(vhost: string = "%2F", name: string): Promise<RabbitConnectionResponse> {
    const r = await got.put<RabbitConnectionResponse>(
      `http://${this.firstNode.host}:${this.port}/api/queues/${vhost}/${name}`,
      {
        json: { arguments: { "x-queue-type": "stream" }, durable: true },
        username: this.username,
        password: this.password,
      }
    )

    return r.body
  }

  async getQueues(): Promise<RabbitQueueResponse[]> {
    const ret = await got.get<RabbitQueueResponse[]>(`http://${this.firstNode.host}:${this.port}/api/queues`, {
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

  async getNodes(): Promise<string[]> {
    const ret = await got.get<{ name: string }[]>(`http://${this.firstNode.host}:${this.port}/api/nodes`, {
      username: this.username,
      password: this.password,
      responseType: "json",
    })
    return ret.body.map((n) => n.name)
  }
}
