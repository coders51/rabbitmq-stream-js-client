import { expect } from "chai"
import { Client, connect } from "../../src"
import { Offset } from "../../src/requests/subscribe_request"
import { getAddressResolverFromEnv } from "../../src/util"
import { createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { getTestNodesFromEnv, password, username, wait } from "../support/util"

describe("address resolver", () => {
  let streamName: string
  const rabbit = new Rabbit(username, password)
  let client: Client

  beforeEach(async () => {
    const [firstNode] = getTestNodesFromEnv()
    const resolver = getAddressResolverFromEnv()
    client = await connect({
      hostname: firstNode.host,
      port: firstNode.port,
      username,
      password,
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
      addressResolver: { enabled: true, endpoint: resolver },
    })
    streamName = createStreamName()
    await rabbit.createStream(streamName)
    // wait for replicas to be created
    await wait(200)
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("declaring a consumer - should not throw", async () => {
    await client.declareConsumer({ stream: streamName, offset: Offset.first() }, () => null)
  })

  it("declaring a consumer - if multiple nodes are present the consumer should be connected to a replica", async () => {
    const consumer = await client.declareConsumer({ stream: streamName, offset: Offset.first() }, () => null)

    const connectionInfo = consumer.getConnectionInfo()
    const queueInfo = await rabbit.getQueueInfo(streamName)
    const nodes = await rabbit.getNodes()
    if (nodes.length > 1) {
      expect(extractNodeName(queueInfo.node)).not.to.be.eql(connectionInfo.host)
    }
  })

  it("declaring a publisher - should not throw", async () => {
    await client.declarePublisher({ stream: streamName })
  })

  it("declaring a publisher - the publisher should be connected to the leader", async () => {
    const publisher = await client.declarePublisher({ stream: streamName })

    const connectionInfo = publisher.getConnectionInfo()
    const queueInfo = await rabbit.getQueueInfo(streamName)
    expect(extractNodeName(queueInfo.node)).to.be.eql(connectionInfo.host)
  })
})

const extractNodeName = (node: string): string => {
  const [_, name] = node.split("@")
  return name
}
