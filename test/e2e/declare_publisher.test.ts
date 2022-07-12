import { expect } from "chai"
import { randomUUID } from "crypto"
import { connect } from "../../src"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync } from "../support/util"

describe("declare publisher", () => {
  const rabbit = new Rabbit()
  const testStreamName = "test-stream"
  const nonExistingStream = "not-the-test-stream"
  const publisherRef = randomUUID()

  beforeEach(async () => {
    await rabbit.createStream(testStreamName)
  })

  afterEach(async () => {
    await rabbit.deleteStream(testStreamName)
  })

  it("declaring a publisher on an existing stream - the publisher should be created", async () => {
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0,
      heartbeat: 0,
    })

    await connection.declarePublisher({ stream: testStreamName, publisherRef })

    await eventually(async () => {
      expect(await rabbit.returnPublishers(testStreamName))
        .lengthOf(1)
        .and.to.include(publisherRef)
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
      () => connection.declarePublisher({ stream: nonExistingStream, publisherRef }),
      Error,
      "Declare Publisher command returned error with code 2"
    )
  })
})
