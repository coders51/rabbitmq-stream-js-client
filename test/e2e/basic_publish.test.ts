import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect, Connection } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"
import * as ampq from "amqplib"

// TODO remove audit issue
// TODO verify if amqplib is install "normally"
// TODO Add ability to encode Properties, ApplicationProperties, Annotations in the message

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
      heartbeat: 0, // not used
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

  it("can be read using classic client", async () => {
    const { publisher, stream } = await createPublisher(rabbit, connection)
    const message = `test${randomUUID()}`
    await publisher.send(BigInt(1), Buffer.from(message))

    const messageFromStream = await getMessageFrom(stream)

    expect(message).eql(messageFromStream)
  })
})

async function createPublisher(rabbit: Rabbit, connection: Connection) {
  const stream = `my-stream-${randomUUID()}`
  await rabbit.createStream(stream)
  const publisher = await connection.declarePublisher({ stream, publisherRef: "my publisher" })
  return { publisher, stream }
}

async function getMessageFrom(stream: string): Promise<string> {
  return new Promise(async (res, rej) => {
    const con = await ampq.connect("amqp://rabbit:rabbit@localhost")
    con.on("error", async (err) => rej(err))
    const ch = await con.createChannel()
    await ch.prefetch(1)
    await ch.consume(
      stream,
      async (msg) => {
        if (!msg) return
        ch.ack(msg)
        console.log(msg.content.toString())
        await ch.close()
        await con.close()
        res(msg.content.toString())
      },
      { arguments: { "x-stream-offset": "first" } }
    )
  })
}
