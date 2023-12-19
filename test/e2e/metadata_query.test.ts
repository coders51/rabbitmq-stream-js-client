import { expect } from "chai"
import { Connection } from "../../src"
import { createConnection, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { username, password } from "../support/util"
import { Broker } from "../../src/responses/metadata_response"

describe("query metadata", () => {
  let streamName: string
  let nonExistingStreamName: string
  const rabbit = new Rabbit(username, password)
  let connection: Connection
  const RABBIT_TESTING_NODES: Broker[] = [
    {
      host: "localhost",
      port: 5552,
      reference: 0,
    },
    {
      host: "rabbitmq",
      port: 5552,
      reference: 0,
    },
  ]

  beforeEach(async () => {
    connection = await createConnection(username, password)
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
    } catch (e) {
      console.error("Error on metadata query test teardown", e)
    }
  })

  it("query the metadata - the response gets parsed correctly and no exception is thrown", async () => {
    await connection.queryMetadata({ streams: [streamName] })
  })

  it("query the metadata - the server should return streamMetaData", async () => {
    const [streamInfo] = await connection.queryMetadata({ streams: [streamName] })

    expect(streamInfo).to.exist
    expect(streamInfo.streamName).to.eql(streamName)
  })

  it("query the metadata - on a non-existing stream the leader or replicas should not be defined", async () => {
    const [streamInfo] = await connection.queryMetadata({ streams: [nonExistingStreamName] })

    expect(streamInfo.streamName).to.eql(nonExistingStreamName)
    expect(streamInfo.leader).not.to.exist
    expect(streamInfo.replicas).to.have.lengthOf(0)
  })

  it("querying the metadata - on an existing stream on a single node", async () => {
    const [streamInfo] = await connection.queryMetadata({ streams: [streamName] })

    expect(streamInfo.streamName).to.eql(streamName)
    expect(streamInfo.responseCode).to.eql(1)
    expect(streamInfo.leader).to.be.deep.oneOf(RABBIT_TESTING_NODES)
    expect(streamInfo.replicas).to.have.lengthOf(0)
  })

  it("querying the metadata - query for multiple streams", async () => {
    const secondStreamName = createStreamName()
    await rabbit.createStream(secondStreamName)

    const res = await connection.queryMetadata({ streams: [streamName, secondStreamName] })
    await rabbit.deleteStream(secondStreamName)

    const firstStreamInfo = res.find((i) => i.streamName === streamName)
    const secondStreamInfo = res.find((i) => i.streamName === secondStreamName)
    expect(firstStreamInfo).to.exist
    expect(firstStreamInfo!.streamName).to.eql(streamName)
    expect(firstStreamInfo!.responseCode).to.eql(1)
    expect(firstStreamInfo!.leader).to.be.deep.oneOf(RABBIT_TESTING_NODES)
    expect(firstStreamInfo!.replicas).to.have.lengthOf(0)
    expect(secondStreamInfo).to.exist
    expect(secondStreamInfo!.streamName).to.eql(secondStreamName)
    expect(secondStreamInfo!.responseCode).to.eql(1)
    expect(secondStreamInfo!.leader).to.be.deep.oneOf(RABBIT_TESTING_NODES)
    expect(secondStreamInfo!.replicas).to.have.lengthOf(0)
  })
})
