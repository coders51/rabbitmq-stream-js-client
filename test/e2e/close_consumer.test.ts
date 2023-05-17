import { expect } from "chai"
import { Connection } from "../../src"
import { Offset } from "../../src/requests/subscribe_request"
import { Rabbit } from "../support/rabbit"
import { expectToThrowAsync, password, username } from "../support/util"
import { createConnection } from "../support/fake_data"

describe("close consumer", () => {
  const rabbit = new Rabbit(username, password)
  const testStreamName = "test-stream"
  let connection: Connection

  beforeEach(async () => {
    await rabbit.createStream(testStreamName)
    connection = await createConnection(username, password)
  })

  afterEach(async () => {
    await connection.close()
    await rabbit.deleteStream(testStreamName)
  })

  it("closing a consumer in an existing stream", async () => {
    await connection.declarePublisher({ stream: testStreamName })
    const consumer = await connection.declareConsumer(
      { stream: testStreamName, offset: Offset.first(), consumerRef: "my_consumer" },
      console.log
    )

    const response = await connection.closeConsumer(consumer.consumerId)

    expect(response).eql(true)
    expect(connection.consumerCounts()).eql(0)
  }).timeout(5000)

  it("closing a non-existing consumer should rise an error", async () => {
    const nonExistingConsumerId = 123456
    await connection.declarePublisher({ stream: testStreamName })

    await expectToThrowAsync(() => connection.closeConsumer(nonExistingConsumerId), Error)
  })
})
