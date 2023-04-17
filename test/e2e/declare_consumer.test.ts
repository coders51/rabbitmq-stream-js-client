import { expect } from "chai"
import { Connection, connect } from "../../src"
import { Message } from "../../src/producer"
import { Offset } from "../../src/requests/subscribe_request"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync } from "../support/util"

describe.skip("declare consumer", () => {
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

  it("declaring a consumer on an existing stream - the consumer should handle the message", async () => {
    const messages: Buffer[] = []
    const publisher = await connection.declarePublisher({ stream: testStreamName })
    await publisher.send(Buffer.from("hello"))

    await connection.declareConsumer({ stream: testStreamName, offset: Offset.first() }, (message: Message) =>
      messages.push(message.content)
    )

    await eventually(() => expect(messages).eql([Buffer.from("hello")]))
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
