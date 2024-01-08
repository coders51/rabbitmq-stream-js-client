import { expect } from "chai"
import { Client } from "../../src"
import { Offset } from "../../src/requests/subscribe_request"
import { Rabbit } from "../support/rabbit"
import { expectToThrowAsync, password, username } from "../support/util"
import { createClient } from "../support/fake_data"

describe("close consumer", () => {
  const rabbit = new Rabbit(username, password)
  const testStreamName = "test-stream"
  let client: Client

  beforeEach(async () => {
    await rabbit.createStream(testStreamName)
    client = await createClient(username, password)
  })

  afterEach(async () => {
    await client.close()
    await rabbit.deleteStream(testStreamName)
  })

  it("closing a consumer in an existing stream", async () => {
    await client.declarePublisher({ stream: testStreamName })
    const consumer = await client.declareConsumer({ stream: testStreamName, offset: Offset.first() }, console.log)

    const response = await client.closeConsumer(consumer.consumerId)

    expect(response).eql(true)
    expect(client.consumerCounts()).eql(0)
  }).timeout(5000)

  it("closing a non-existing consumer should rise an error", async () => {
    const nonExistingConsumerId = 123456
    await client.declarePublisher({ stream: testStreamName })

    await expectToThrowAsync(() => client.closeConsumer(nonExistingConsumerId), Error)
  })
})
