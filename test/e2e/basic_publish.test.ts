import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect, Connection } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"
import * as ampq from "amqplib"
import { MessageProperties } from "../../src/producer"

describe("publish a message", () => {
  const rabbit = new Rabbit()
  let connection: Connection

  beforeEach(async () => {
    connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0, // not used
      heartbeat: 0,
    })
  })
  afterEach(() => connection.close())
  afterEach(() => rabbit.closeAllConnections())
  afterEach(() => rabbit.deleteAllQueues({ match: /my-stream-/ }))

  it("is seen by rabbit", async () => {
    const stream = `my-stream-${randomUUID()}`
    await rabbit.createStream(stream)
    const publisher = await connection.declarePublisher({ stream, publisherRef: "my publisher" })

    await publisher.send(1n, Buffer.from(`test${randomUUID()}`))

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(1)
    }, 10000)
  }).timeout(10000)

  it("and a lot more are all seen by rabbit", async () => {
    const { publisher, stream } = await createPublisher(rabbit, connection)

    for (let index = 0; index < 100; index++) {
      await publisher.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
    }

    await eventually(async () => {
      expect((await rabbit.getQueueInfo(stream)).messages).eql(100)
    }, 10000)
  }).timeout(30000)

  describe("deduplication", () => {
    it("is active if create a publisher with publishRef", async () => {
      const stream = `my-stream-${randomUUID()}`
      await rabbit.createStream(stream)
      const publisher = await connection.declarePublisher({ stream, publisherRef: "this-producer" })

      const howMany = 100
      for (let index = 0; index < howMany; index++) {
        await publisher.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
      }
      for (let index = 0; index < howMany; index++) {
        await publisher.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
      }

      await eventually(async () => {
        expect((await rabbit.getQueueInfo(stream)).messages).eql(howMany)
      }, 10000)
      await connection.close()
    }).timeout(30000)

    it("is not active if create a publisher without publishRef", async () => {
      const stream = `my-stream-${randomUUID()}`
      await rabbit.createStream(stream)
      const publisher = await connection.declarePublisher({ stream })

      const howMany = 100
      for (let index = 0; index < howMany; index++) {
        await publisher.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
      }
      for (let index = 0; index < howMany; index++) {
        await publisher.send(BigInt(index), Buffer.from(`test${randomUUID()}`))
      }

      await eventually(async () => {
        expect((await rabbit.getQueueInfo(stream)).messages).eql(howMany * 2)
      }, 10000)
      await connection.close()
    }).timeout(30000)
  })
})

function createProperties(): MessageProperties {
  return {
    contentType: `contentType`,
    contentEncoding: `contentEncoding`,
    replyTo: `replyTo`,
    to: `to`,
    subject: `subject`,
    correlationId: `correlationIdAAA`,
    messageId: `messageId`,
    userId: Buffer.from(`userId`),
    absoluteExpiryTime: new Date(),
    creationTime: new Date(),
    groupId: `groupId`,
    groupSequence: 666,
    replyToGroupId: `replyToGroupId`,
  }
}

async function createPublisher(rabbit: Rabbit, connection: Connection) {
  const stream = `my-stream-${randomUUID()}`
  await rabbit.createStream(stream)
  const publisher = await connection.declarePublisher({ stream, publisherRef: "my publisher" })
  return { publisher, stream }
}

async function getMessageFrom(stream: string): Promise<{ content: string; properties: ampq.MessageProperties }> {
  return new Promise(async (res, rej) => {
    const con = await ampq.connect("amqp://rabbit:rabbit@localhost")
    con.on("error", async (err) => rej(err))
    const ch = await con.createChannel()
    await ch.prefetch(1)
    await ch.consume(
      stream,
      async (msg) => {
        if (!msg) return
        msg.properties.userId
        ch.ack(msg)
        await ch.close()
        await con.close()
        res({ content: msg.content.toString(), properties: msg.properties })
      },
      { arguments: { "x-stream-offset": "first" } }
    )
  })
}
