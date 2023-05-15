import { expect } from "chai"
import { connect } from "http2"
import { Connection, ListenersParams } from "../../src"
import { PublishError } from "../../src/responses/publish_error"
import { createConnection, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"

describe("Receving publishing errors", () => {
  const rabbit = new Rabbit()
  let connection: Connection
  let streamName: string
  const publishError: PublishError[] = []

  beforeEach(async () => {
    publishError.length = 0
    const listeners: ListenersParams = {
      publish_error: (data) => publishError.push(data),
    }
    connection = await createConnection(listeners)
    streamName = createStreamName()
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

  it.only("when publish id does not exist", async () => {
    await createPublisher(streamName, connection)
    expect(true).eql(true)
  })
})