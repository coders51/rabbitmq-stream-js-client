import { expect } from "chai"
import { Connection } from "../../src"
import { Producer } from "../../src/producer"
import { createConnection, createPublisher, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, username, password } from "../support/util"
import { NoneCompression } from "../../src/compression"

describe("publish a batch of messages", () => {
  const rabbit = new Rabbit(username, password)
  let connection: Connection
  let streamName: string
  let publisher: Producer

  beforeEach(async () => {
    connection = await createConnection(username, password)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
    publisher = await createPublisher(streamName, connection)
  })

  afterEach(async () => {
    try {
      await connection.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  it("publish a batch of messages", async () => {
    await publisher.sendSubEntries(
      [
        { content: Buffer.from("Ciao") },
        { content: Buffer.from("Ciao1") },
        { content: Buffer.from("Ciao2") },
        { content: Buffer.from("Ciao3") },
      ],
      NoneCompression.create()
    )

    await eventually(async () => {
      const info = await rabbit.getQueueInfo(streamName)
      expect(info.messages).eql(1)
    }, 10000)
  }).timeout(10000)
})
