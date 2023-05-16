import { connect } from "../../src"
import { expect } from "chai"
import { Rabbit } from "../support/rabbit"
import { randomUUID } from "crypto"
import { expectToThrowAsync, username, password } from "../support/util"

describe("DeletePublisher command", () => {
  const rabbit = new Rabbit(username, password)
  const testStreamName = "test-stream"
  let publisherRef: string

  beforeEach(async () => {
    publisherRef = randomUUID()
    await rabbit.createStream(testStreamName)
  })

  afterEach(() => rabbit.deleteStream(testStreamName))

  it("can delete a publisher", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username,
      password,
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })
    const publisher = await connection.declarePublisher({ stream: testStreamName, publisherRef })
    await publisher.send(Buffer.from(`test${randomUUID()}`))

    const publisherId = publisher.getPublisherId()

    const deletePublisher = await connection.deletePublisher(Number(publisherId))
    expect(deletePublisher).eql(true)

    await connection.close()
  }).timeout(10000)

  it("errors when deleting a publisher that does not exist", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username,
      password,
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })
    const nonExistentPublisherId = 42

    await expectToThrowAsync(
      () => connection.deletePublisher(Number(nonExistentPublisherId)),
      Error,
      "Publisher with id: 42 does not exist"
    )

    await connection.close()
  }).timeout(10000)
})
