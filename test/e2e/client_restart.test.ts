import { expect } from "chai"
import { Client, Offset } from "../../src"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"
import { Consumer } from "../../src/consumer"
import { Message, Publisher } from "../../src/publisher"

describe.only("restart connections", () => {
  const rabbit = new Rabbit(username, password)
  let streamName: string
  let client: Client

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("client only, no producer or consumer", async () => {
    await eventually(async () => {
      if (!client.getConnectionInfo().ready) {
        console.log(`not ready yet`)
        throw new Error("not ready yet")
      }
    })
    const oldConnectionInfo = client.getConnectionInfo()

    await client.restart()

    await eventually(async () => {
      const connectionInfo = client.getConnectionInfo()
      expect(connectionInfo.ready).eql(true)
      expect(connectionInfo.localPort).not.undefined
      expect(oldConnectionInfo.localPort).not.eql(connectionInfo.localPort)
    })
  }).timeout(10000)

  it("client with publishers and consumers", async () => {
    await eventually(async () => {
      if (!client.getConnectionInfo().ready) {
        console.log(`not ready yet`)
        throw new Error("not ready yet")
      }
    })
    const clientOldConnectionInfo = client.getConnectionInfo()
    const streamNames = [streamName, createStreamName(), createStreamName()]
    const publishers = new Map<number, Publisher>()
    const localPublisherPorts = new Map<number, number>()
    const localConsumerPorts = new Map<number, number>()
    const consumers = new Map<number, Consumer>()
    const dummyMsgHandler = (_msg: Message) => {
      return
    }
    for (const streamName of streamNames) {
      await rabbit.createStream(streamName)
      const publisher = await client.declarePublisher({ stream: streamName })
      const consumer1 = await client.declareConsumer({ stream: streamName, offset: Offset.first() }, dummyMsgHandler)
      const consumer2 = await client.declareConsumer({ stream: streamName, offset: Offset.first() }, dummyMsgHandler)
      publishers.set(publisher.publisherId, publisher)
      consumers.set(consumer1.consumerId, consumer1)
      consumers.set(consumer2.consumerId, consumer2)
      localPublisherPorts.set(publisher.publisherId, publisher.getConnectionInfo().localPort!)
      localConsumerPorts.set(consumer1.consumerId, consumer1.getConnectionInfo().localPort!)
      localConsumerPorts.set(consumer2.consumerId, consumer2.getConnectionInfo().localPort!)
    }

    await client.restart()

    await eventually(async () => {
      const connectionInfo = client.getConnectionInfo()
      expect(connectionInfo.localPort).is.not.undefined
      expect(connectionInfo.localPort).not.eql(clientOldConnectionInfo.localPort)
      expect(connectionInfo.ready).eql(true)
      for (const consumerId of consumers.keys()) {
        const consumer = consumers.get(consumerId)
        const connectionInfo = consumer!.getConnectionInfo()
        expect(connectionInfo.ready).eql(true)
        expect(connectionInfo.localPort).not.undefined
        expect(connectionInfo.localPort).not.eql(localConsumerPorts.get(consumerId))
      }
      for (const publisherId of publishers.keys()) {
        const publisher = publishers.get(publisherId)
        const connectionInfo = publisher!.getConnectionInfo()
        expect(connectionInfo.ready).eql(true)
        expect(connectionInfo.localPort).not.undefined
        expect(connectionInfo.localPort).not.eql(localPublisherPorts.get(publisherId))
      }
    }, 10000)
  }).timeout(20000)
})
