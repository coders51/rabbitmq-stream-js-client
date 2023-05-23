import { Connection } from "../../src"
import { expect } from "chai"
import { Rabbit } from "../support/rabbit"
import { randomUUID } from "crypto"
import { username, password } from "../support/util"
import { createConnection } from "../support/fake_data"

describe.only("ExchangeCommandVersions", () => {
  const rabbit = new Rabbit(username, password)
  const testStreamName = "test-stream"
  let connection: Connection
  let publisherRef: string

  beforeEach(async () => {
    publisherRef = randomUUID()
    await rabbit.createStream(testStreamName)
    connection = await createConnection(username, password)
  })

  afterEach(async () => {
    await connection.close()
    await rabbit.deleteStream(testStreamName)
  })

  it("gets min and max versions", async () => {
    const publisher = await connection.declarePublisher({ stream: testStreamName, publisherRef })
    await publisher.send(Buffer.from(`test${randomUUID()}`))

    const exchangeCommandVersions = await connection.exchangeCommandVersions()
    expect(exchangeCommandVersions.minVersion).eql(1)
    expect(exchangeCommandVersions.maxVersion).eql(1)
  })
})
