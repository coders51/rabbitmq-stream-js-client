import { expect } from "chai"
import { connect } from "../../src"
import { Offset } from "../../src/requests/subscribe_request"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync } from "../support/util"

describe("declare consumer", () => {
  const rabbit = new Rabbit()
  const testStreamName = "test-stream"
  const nonExistingStream = "not-the-test-stream"
  const emptyPublisherRef = ""

  beforeEach(async () => {
    await rabbit.createStream(testStreamName)
  })

  afterEach(async () => {
    await rabbit.deleteStream(testStreamName)
  })

  it("when a consumer is created, Rabbit should show it if asked", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })

    await connection.declareConsumer({ stream: testStreamName, offset: Offset.first() }, (message: any) =>
      console.log(message)
    )

    await eventually(async () => {
      expect(await rabbit.returnConsumers()).lengthOf(1)
    }, 5000)
    await connection.close()
  }).timeout(10000)

  it.only("declaring a consumer on an existing stream - the consumer should be handle the message", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })
    const messages: string[] = []
    const publisher = await connection.declarePublisher({ stream: testStreamName })
    await publisher.send(Buffer.from("hello"))

    await connection.declareConsumer({ stream: testStreamName, offset: Offset.first() }, (message: any) =>
      messages.push(message)
    )

    await eventually(() => expect(messages).eql(["hello"]))
    await connection.close()
  }).timeout(10000)

  it("declaring a publisher on an existing stream with no publisherRef - the publisher should be created", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })

    await connection.declareConsumer({ stream: testStreamName, offset: Offset.first() })

    await eventually(async () => {
      expect(await rabbit.returnPublishers(testStreamName))
        .lengthOf(1)
        .and.to.include(emptyPublisherRef)
    }, 5000)
    await connection.close()
  }).timeout(10000)

  it("declaring a publisher on a non-existing stream should raise an error", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })

    await expectToThrowAsync(
      () => connection.declareConsumer({ stream: nonExistingStream, offset: Offset.first() }),
      Error,
      "Declare Publisher command returned error with code 2 - Stream does not exist"
    )

    await connection.close()
  })
})
