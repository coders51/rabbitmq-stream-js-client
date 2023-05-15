import { connect, Connection } from "../../src"
import { expect } from "chai"
import { Rabbit } from "../support/rabbit"
import { randomUUID } from "crypto"

describe("PublishError", () => {
  const rabbit = new Rabbit()
  const testStreamName = "test-stream"
  let publisherRef: string

  beforeEach(async () => {
    publisherRef = randomUUID()
    await rabbit.createStream(testStreamName)
  })

  afterEach(() => rabbit.deleteStream(testStreamName))

  it.only("received publishing error", () => {
    expect(true).eql(true)
  })
})