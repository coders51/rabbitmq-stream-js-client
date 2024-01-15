import { expect } from "chai"
import { Client } from "../../src"
import { Rabbit } from "../support/rabbit"
import { password, username } from "../support/util"
import { createClient, createPublisher } from "../support/fake_data"

describe("close publisher", () => {
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

  it("closing a publisher", async () => {
    const publisher = await client.declarePublisher({ stream: testStreamName })

    const response = await client.deletePublisher(publisher.publisherId)

    const publisherInfo = publisher.getConnectionInfo()
    expect(response).eql(true)

    expect(publisherInfo.writable).eql(false)
  }).timeout(5000)

  it("closing a publisher does not close the underlying connection if it is still in use", async () => {
    const publisher1 = await createPublisher(testStreamName, client)
    const publisher2 = await createPublisher(testStreamName, client)

    await client.deletePublisher(publisher1.publisherId)

    const publisher2Info = publisher2.getConnectionInfo()
    expect(publisher2Info.writable).eql(true)
  })

  it("closing all publishers sharing a connection also close the connection", async () => {
    const publisher1 = await createPublisher(testStreamName, client)
    const publisher2 = await createPublisher(testStreamName, client)

    await client.deletePublisher(publisher1.publisherId)
    await client.deletePublisher(publisher2.publisherId)

    const publisher1Info = publisher1.getConnectionInfo()
    const publisher2Info = publisher2.getConnectionInfo()
    expect(publisher1Info.writable).eql(false)
    expect(publisher2Info.writable).eql(false)
  })
})
