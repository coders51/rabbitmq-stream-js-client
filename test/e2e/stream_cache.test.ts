import { expect } from "chai"
import got from "got"
import { Client, Publisher } from "../../src"
import {
  createClient,
  createStreamName,
} from "../support/fake_data"
import { Rabbit, RabbitConnectionResponse } from "../support/rabbit"
import {
  getTestNodesFromEnv,
  password,
  username,
} from "../support/util"

async function createVhost(vhost: string): Promise<RabbitConnectionResponse> {
  const port = process.env.RABBIT_MQ_MANAGEMENT_PORT || 15672
  const firstNode = getTestNodesFromEnv().shift()!
  await got.put<RabbitConnectionResponse>(
    `http://${firstNode.host}:${port}/api/vhosts/${vhost}`,
    {
      username: username,
      password: password,
    }
  )
  const res = await got.put<RabbitConnectionResponse>(
    `http://${firstNode.host}:${port}/api/permissions/${vhost}/${username}`,
    {
      json: {
        read: '.*',
        write: '.*',
        configure: '.*'
      },
      username: username,
      password: password,
    }
  ).json()
}

async function deleteVhost(vhost: string): Promise<RabbitConnectionResponse> {
  const port = process.env.RABBIT_MQ_MANAGEMENT_PORT || 15672
  const firstNode = getTestNodesFromEnv().shift()!
  const r = await got.delete<RabbitConnectionResponse>(
    `http://${firstNode.host}:${port}/api/vhosts/${vhost}`,
    {
      username: username,
      password: password,
    }
  )

  return r.body
}

describe("cache", () => {
  const vhost1 = "vhost1"
  let streamName: string
  const rabbit = new Rabbit(username, password)
  let client: Client
  let client2: Client
  before(async () => {
    await createVhost(vhost1)
  })
  beforeEach(async () => {
    client = await createClient(username, password)
    client2 = await createClient(username, password, null, null, null, null, null, vhost1)
    streamName = createStreamName()
    await client.createStream({ stream: streamName })
    await client2.createStream({ stream: streamName })
  })
  afterEach(async () => {
    try {
      await client.close()
      await client2.close()
      await deleteVhost(vhost1)
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (_e) { }
  })

  it("should cache using the vhost as well as the stream name", async () => {
    const publisher1 = await client.declarePublisher({
      stream: streamName,
    })
    expect(publisher1.getConnectionInfo().vhost).eql("/")
    const publisher2 = await client2.declarePublisher({
      stream: streamName,
    })
    expect(publisher2.getConnectionInfo().vhost).eql(vhost1)
  })
})
