import { expect } from "chai"
import { Connection, connect } from "../../src"
import { Message } from "../../src/producer"
import { Offset } from "../../src/requests/subscribe_request"
import { createClassicConsumer } from "../../src/util"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync } from "../support/util"

describe("declare consumer", () => {
  const rabbit = new Rabbit()
  const testStreamName = "test-stream"
  const nonExistingStream = "not-the-test-stream"
  let connection: Connection

  beforeEach(async () => {
    try {
      await rabbit.deleteStream(testStreamName)
    } catch (error) {}
    await rabbit.createStream(testStreamName)
    connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })
  })

  afterEach(async () => {
    await connection.close()
    await rabbit.deleteStream(testStreamName)
  })

  it("should handle the message", async () => {
    const messages: string[] = []
    await connection.declareConsumer({ stream: testStreamName, offset: Offset.next() }, (message: Message) =>
      messages.push(message.content.toString())
    )

    const publisher = await connection.declarePublisher({ stream: testStreamName })
    await publisher.send(Buffer.from("hello"))

    await eventually(() => expect(messages).eql(["hello"]))
  }).timeout(10000)

  it("declaring a consumer on an existing stream - the consumer should be handle more then one message", async () => {
    const messages: Buffer[] = []
    const publisher = await connection.declarePublisher({ stream: testStreamName })
    await publisher.send(Buffer.from("hello"))
    await publisher.send(Buffer.from("world"))

    await connection.declareConsumer({ stream: testStreamName, offset: Offset.first() }, (message: Message) =>
      messages.push(message.content)
    )

    await eventually(() => expect(messages).eql([Buffer.from("hello"), Buffer.from("world")]))
    await eventually(() => expect(messages).eql([Buffer.from("hello"), Buffer.from("world")]))
  }).timeout(10000)

  it(`consume a lot of messages`, async () => {
    const receivedMessages: string[] = []
    const receivedMessages1: string[] = []

    await connection.declareConsumer({ stream: testStreamName, offset: Offset.next() }, (message: Message) => {
      receivedMessages.push(message.content.toString())
    })

    const publisher = await connection.declarePublisher({ stream: testStreamName })
    const messages = range(1000).map((n) => "hello" + n)
    for (const m of messages) {
      await publisher.send(Buffer.from(m))
    }

    const { conn, ch } = await createClassicConsumer(testStreamName, (m) =>
      receivedMessages1.push(m.content.toString())
    )
    await eventually(() => expect(receivedMessages1).eql(messages), 7000)
    await ch.close()
    await conn.close()
    await eventually(() => expect(receivedMessages).eql(messages), 7000)
  }).timeout(50000)

  it("declaring a consumer on a non-existing stream should raise an error", async () => {
    await expectToThrowAsync(
      () =>
        connection.declareConsumer({ stream: nonExistingStream, offset: Offset.first() }, (message: Message) =>
          console.log(message.content)
        ),
      Error,
      "Declare Consumer command returned error with code 2 - Stream does not exist"
    )
  })
})

function range(count: number): number[] {
  const ret = Array(count)
  for (let index = 0; index < count; index++) {
    ret[index] = index
  }
  return ret
}
