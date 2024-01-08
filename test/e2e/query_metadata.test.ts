import { expect } from "chai"
import { Client } from "../../src"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { getTestNodesFromEnv, password, username } from "../support/util"

describe("query metadata", () => {
  let streamName: string
  let nonExistingStreamName: string
  const rabbit = new Rabbit(username, password)
  let client: Client
  const nodes = getTestNodesFromEnv()
  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    nonExistingStreamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {
      console.error("Error on metadata query test teardown", e)
    }
  })

  it("query the metadata - the response gets parsed correctly and no exception is thrown", async () => {
    await client.queryMetadata({ streams: [streamName] })
  })

  it("query the metadata - the server should return streamMetaData", async () => {
    const [streamInfo] = await client.queryMetadata({ streams: [streamName] })

    expect(streamInfo).to.exist
    expect(streamInfo.streamName).to.eql(streamName)
  })

  it("query the metadata - on a non-existing stream the leader or replicas should not be defined", async () => {
    const [streamInfo] = await client.queryMetadata({ streams: [nonExistingStreamName] })

    expect(streamInfo.streamName).to.eql(nonExistingStreamName)
    expect(streamInfo.leader).not.to.exist
    expect(streamInfo.replicas).to.have.lengthOf(0)
  })

  it("querying the metadata - on an existing stream on a single node", async () => {
    const [streamInfo] = await client.queryMetadata({ streams: [streamName] })

    expect(streamInfo.streamName).to.eql(streamName)
    expect(streamInfo.responseCode).to.eql(1)
    expect({ host: streamInfo.leader?.host, port: streamInfo.leader?.port }).to.be.deep.oneOf(nodes)
  })

  it("querying the metadata - query for multiple streams", async () => {
    const secondStreamName = createStreamName()
    await rabbit.createStream(secondStreamName)

    const res = await client.queryMetadata({ streams: [streamName, secondStreamName] })
    await rabbit.deleteStream(secondStreamName)

    const firstStreamInfo = res.find((i) => i.streamName === streamName)
    const secondStreamInfo = res.find((i) => i.streamName === secondStreamName)
    expect(firstStreamInfo).to.exist
    expect(firstStreamInfo!.streamName).to.eql(streamName)
    expect(firstStreamInfo!.responseCode).to.eql(1)
    expect({ host: firstStreamInfo!.leader?.host, port: firstStreamInfo!.leader?.port }).to.be.deep.oneOf(nodes)
    expect(secondStreamInfo).to.exist
    expect(secondStreamInfo!.streamName).to.eql(secondStreamName)
    expect(secondStreamInfo!.responseCode).to.eql(1)
    expect({ host: secondStreamInfo!.leader?.host, port: secondStreamInfo!.leader?.port }).to.be.deep.oneOf(nodes)
  })
})
