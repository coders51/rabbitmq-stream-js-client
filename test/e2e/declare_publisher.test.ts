import { expect } from "chai"
import { Connection } from "../../src"
import { createConnection, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, expectToThrowAsync } from "../support/util"

describe("declare publisher", () => {
  const rabbit = new Rabbit()
  let streamName: string
  let nonExistingStreamName: string
  let connection: Connection

  beforeEach(async () => {
    connection = await createConnection()
    streamName = createStreamName()
    nonExistingStreamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    try {
      await connection.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("declaring a publisher on an existing stream - the publisher should be created", async () => {
    const producer = await createPublisher(streamName, connection)

    await eventually(async () => {
      expect(await rabbit.returnPublishers(streamName))
        .lengthOf(1)
        .and.to.include(producer.ref)
    }, 5000)
  }).timeout(10000)

  it("declaring a publisher on an existing stream with no publisherRef - the publisher should be created", async () => {
    const producer = await createPublisher(streamName, connection)

    await eventually(async () => {
      expect(await rabbit.returnPublishers(streamName))
        .lengthOf(1)
        .and.to.include(producer.ref)
    }, 5000)
  }).timeout(10000)

  it("declaring a publisher on a non-existing stream should raise an error", async () => {
    await expectToThrowAsync(
      () => createPublisher(nonExistingStreamName, connection),
      Error,
      "Declare Publisher command returned error with code 2 - Stream does not exist"
    )
  })
})
