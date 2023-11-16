import { Connection } from "../../src"
import { expect } from "chai"
import { Rabbit } from "../support/rabbit"
import { randomUUID } from "crypto"
import { expectToThrowAsync, username, password } from "../support/util"
import { createConnection } from "../support/fake_data"

describe("DeletePublisher command", () => {
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

  it("can delete a publisher", async () => {
    const publisher = await connection.declarePublisher({ stream: testStreamName, publisherRef })
    await publisher.send(Buffer.from(`test${randomUUID()}`))

    const publisherId = publisher.publisherId

    const deletePublisher = await connection.deletePublisher(Number(publisherId))
    expect(deletePublisher).eql(true)
  }).timeout(10000)

  it("errors when deleting a publisher that does not exist", async () => {
    const nonExistentPublisherId = 42

    await expectToThrowAsync(
      () => connection.deletePublisher(Number(nonExistentPublisherId)),
      Error,
      "Delete Publisher command returned error with code 18 - Publisher does not exist",
    )
  }).timeout(10000)
})
