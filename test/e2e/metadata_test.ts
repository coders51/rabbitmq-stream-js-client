import { expect } from "chai"
import { Rabbit } from "../support/rabbit"
import { createConnection, createPublisher, createStreamName } from "../support/fake_data"
import { eventually, username, password } from "../support/util"
import { Connection } from "../../src"
// MetadataQuery => Key Version CorrelationId [Stream]
//   Key => uint16 // 0x000f
//   Version => uint16
//   CorrelationId => uint32
//   Stream => string

// MetadataResponse => Key Version CorrelationId [Broker] [StreamMetadata]
//   Key => uint16 // 0x800f
//   Version => uint16
//   CorrelationId => uint32
//   Broker => Reference Host Port
//     Reference => uint16
//     Host => string
//     Port => uint32
//   StreamMetadata => StreamName ResponseCode LeaderReference ReplicasReferences
//      StreamName => string
//      ResponseCode => uint16
//      LeaderReference => uint16
//      ReplicasReferences => [uint16]
describe("metadataQuery", () => {
  const rabbit = new Rabbit(username, password)
  let streamName: string
  let connection: Connection

  beforeEach(async () => {
    connection = await createConnection(username, password)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    await connection.close()
    await rabbit.deleteStream(streamName)
  })

  it.only("gets metadata for a stream", async () => {
    await createPublisher(streamName, connection)
    const res = await connection.metadataQuery(streamName)
    expect(res).eql("foo")
  }).timeout(10000)
})
