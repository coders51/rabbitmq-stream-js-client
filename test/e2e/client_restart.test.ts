import { expect } from "chai"
import { Client, Offset } from "../../src"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"
import { Consumer } from "../../src/consumer"
import { Message, Publisher } from "../../src/publisher"
import { randomUUID } from "crypto"
import { Socket } from "net"

type ConnectionInternals = { connectionId: string; socket: Socket }
type ClientInternals = { locatorConnection: ConnectionInternals }
type ConsumerInternals = { connection: ConnectionInternals }

const locatorOf = (client: Client) => (client as unknown as ClientInternals).locatorConnection
const connectionOf = (consumer: Consumer) => (consumer as unknown as ConsumerInternals).connection

describe("restart connections", () => {
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

  it("client only, no producer or consumer, socket connection is reestablished", async () => {
    const oldConnectionInfo = client.getConnectionInfo()

    await client.restart()

    await eventually(async () => {
      const connectionInfo = client.getConnectionInfo()
      expect(connectionInfo.ready).eql(true)
      expect(connectionInfo.localPort).not.undefined
      expect(oldConnectionInfo.localPort).not.eql(connectionInfo.localPort)
    })
  }).timeout(10000)

  it("restarting a live connection destroys the previous socket instead of leaking it", async () => {
    const consumer = await client.declareConsumer({ stream: streamName, offset: Offset.first() }, () => undefined)
    const locatorSocket = locatorOf(client).socket
    const consumerSocket = connectionOf(consumer).socket
    expect(locatorSocket.destroyed).eql(false)
    expect(consumerSocket.destroyed).eql(false)

    await client.restart()

    expect(locatorSocket.destroyed).eql(true)
    expect(consumerSocket.destroyed).eql(true)
    expect(locatorOf(client).socket).not.eql(locatorSocket)
  }).timeout(10000)

  it("restarting by connection id only restarts that connection and its consumers", async () => {
    const otherStream = createStreamName()
    await rabbit.createStream(otherStream)
    const messages1: string[] = []
    const messages2: string[] = []
    const consumer1 = await client.declareConsumer({ stream: streamName, offset: Offset.next() }, (m) => {
      messages1.push(m.content.toString())
    })
    const consumer2 = await client.declareConsumer({ stream: otherStream, offset: Offset.next() }, (m) => {
      messages2.push(m.content.toString())
    })
    const connection1 = connectionOf(consumer1)
    const connection2 = connectionOf(consumer2)
    expect(connection1.connectionId).not.eql(connection2.connectionId)
    const locatorSocket = locatorOf(client).socket
    const socket1 = connection1.socket
    const socket2 = connection2.socket

    await client.restart([connection1.connectionId])

    expect(socket1.destroyed).eql(true)
    expect(connection1.socket).not.eql(socket1)
    expect(socket2.destroyed).eql(false)
    expect(connection2.socket).eql(socket2)
    expect(locatorSocket.destroyed).eql(false)

    // both consumers still receive messages
    const publisher1 = await client.declarePublisher({ stream: streamName })
    const publisher2 = await client.declarePublisher({ stream: otherStream })
    await publisher1.send(Buffer.from("one"))
    await publisher2.send(Buffer.from("two"))
    await eventually(() => {
      expect(messages1).eql(["one"])
      expect(messages2).eql(["two"])
    })
    await rabbit.deleteStream(otherStream)
  }).timeout(20000)

  it("restarting with an unknown connection id does nothing", async () => {
    const locatorSocket = locatorOf(client).socket

    await client.restart(["no-such-connection"])

    expect(locatorOf(client).socket).eql(locatorSocket)
    expect(locatorSocket.destroyed).eql(false)
  }).timeout(10000)

  it("client with publishers and consumers, socket connections are reestablished", async () => {
    const clientOldConnectionInfo = client.getConnectionInfo()
    const streamNames = [streamName, createStreamName(), createStreamName()]
    const publishers = new Map<number, Publisher>()
    const localPublisherPorts = new Map<number, number>()
    const localConsumerPorts = new Map<number, number>()
    const consumers = new Map<number, Consumer>()
    const dummyMsgHandler = (_msg: Message) => {
      return
    }
    for (const stream of streamNames) {
      await rabbit.createStream(stream)
      const publisher = await client.declarePublisher({ stream: stream })
      const consumer1 = await client.declareConsumer({ stream: stream, offset: Offset.first() }, dummyMsgHandler)
      const consumer2 = await client.declareConsumer({ stream: stream, offset: Offset.first() }, dummyMsgHandler)
      publishers.set(publisher.publisherId, publisher)
      consumers.set(consumer1.consumerId, consumer1)
      consumers.set(consumer2.consumerId, consumer2)
      localPublisherPorts.set(publisher.publisherId, publisher.getConnectionInfo().localPort!)
      localConsumerPorts.set(consumer1.consumerId, consumer1.getConnectionInfo().localPort!)
      localConsumerPorts.set(consumer2.consumerId, consumer2.getConnectionInfo().localPort!)
    }

    await client.restart()

    await eventually(async () => {
      const clientConnectionInfo = client.getConnectionInfo()
      expect(clientConnectionInfo.localPort).is.not.undefined
      expect(clientConnectionInfo.localPort).not.eql(clientOldConnectionInfo.localPort)
      expect(clientConnectionInfo.ready).eql(true)
      for (const consumerId of consumers.keys()) {
        const consumer = consumers.get(consumerId)
        const consumerConnectionInfo = consumer!.getConnectionInfo()
        expect(consumerConnectionInfo.ready).eql(true)
        expect(consumerConnectionInfo.localPort).not.undefined
        expect(consumerConnectionInfo.localPort).not.eql(localConsumerPorts.get(consumerId))
      }
      for (const publisherId of publishers.keys()) {
        const publisher = publishers.get(publisherId)
        const publisherConnectionInfo = publisher!.getConnectionInfo()
        expect(publisherConnectionInfo.ready).eql(true)
        expect(publisherConnectionInfo.localPort).not.undefined
        expect(publisherConnectionInfo.localPort).not.eql(localPublisherPorts.get(publisherId))
      }
    }, 10000)
  }).timeout(20000)

  it("sending and receiving messages is not affected", async () => {
    const received = new Set<string>()
    const messageNumber = 10000
    const triggerIndex = Math.floor(messageNumber / 4)
    const consumeHandle = (msg: Message) => {
      received.add(msg.messageProperties?.messageId!)
    }
    const oldClientConnectionInfo = client.getConnectionInfo()
    await client.declareConsumer({ stream: streamName, offset: Offset.first() }, consumeHandle)
    const publisher = await client.declarePublisher({ stream: streamName })

    for (let i = 0; i < messageNumber; i++) {
      const msg = Buffer.from(`${randomUUID()}`)
      await publisher.send(msg, { messageProperties: { messageId: `${i}` } })
      if (i === triggerIndex) await client.restart()
    }

    await eventually(async () => {
      const clientConnectionInfo = client.getConnectionInfo()
      expect(clientConnectionInfo.ready).eql(true)
      expect(clientConnectionInfo.localPort).not.eql(oldClientConnectionInfo.localPort)
      expect(received.size).eql(messageNumber)
    }, 10000)
  }).timeout(20000)
})
