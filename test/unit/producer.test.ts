import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect } from "../../src"
import { createConnection } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { password, username } from "../support/util"

describe("Producer", () => {
  const rabbit = new Rabbit(username, password)
  const testStreamName = "test-stream"
  let publisherRef: string

  beforeEach(async () => {
    publisherRef = randomUUID()
    await rabbit.createStream(testStreamName)
  })

  afterEach(() => rabbit.deleteStream(testStreamName))

  it("increase publishing id from server when boot is true", async () => {
    const oldConnection = await createConnection(username, password)
    const oldPublisher = await oldConnection.declarePublisher({ stream: testStreamName, publisherRef })
    const oldMessages = [...Array(3).keys()]
    await Promise.all(oldMessages.map(() => oldPublisher.send(Buffer.from(`test${randomUUID()}`))))
    await oldConnection.close()
    const newConnection = await createConnection(username, password)

    const newPublisher = await newConnection.declarePublisher({ stream: testStreamName, publisherRef, boot: true })
    await newPublisher.send(Buffer.from(`test${randomUUID()}`))

    expect(await newPublisher.getLastPublishingId()).eql(BigInt(oldMessages.length) + 1n)
    await newConnection.close()
  }).timeout(10000)

  it("do not increase publishing id from server when boot is false", async () => {
    const oldConnection = await createConnection(username, password)
    const oldPublisher = await oldConnection.declarePublisher({ stream: testStreamName, publisherRef })
    const oldMessages = [...Array(3).keys()]
    await Promise.all(oldMessages.map(() => oldPublisher.send(Buffer.from(`test${randomUUID()}`))))
    await oldConnection.close()
    const newConnection = await createConnection(username, password)

    const newPublisher = await newConnection.declarePublisher({ stream: testStreamName, publisherRef, boot: false })
    await newPublisher.send(Buffer.from(`test${randomUUID()}`))

    expect(await newPublisher.getLastPublishingId()).eql(BigInt(oldMessages.length))
    await newConnection.close()
  }).timeout(10000)
})
