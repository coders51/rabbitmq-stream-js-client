import { expect } from "chai"
import { connect } from "../../src"
import { CreditResponse } from "../../src/responses/credit_response"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"
import { Offset } from "../../src/requests/subscribe_request"

describe("update the metadata from the server", () => {
  const rabbit = new Rabbit()
  const streamName = "test-stream"
  const creditResponses: CreditResponse[] = []

  beforeEach(async () => await rabbit.createStream(streamName))

  afterEach(async () => await rabbit.deleteStream(streamName))

  it("ask for credit after subscribing to the next message", async () => {
    creditResponses.length = 0
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0, // not used
      heartbeat: 0, // not used
      listeners: {
        metadata_update: (_data) => console.info("Subscribe server error"),
        credit: (data) => creditResponses.push(data),
      },
    })
    await connection.subscribe({
      subscriptionId: 1,
      stream: streamName,
      offset: Offset.next(),
      credit: 1,
    })

    await connection.askForCredit({ subscriptionId: 1, credit: 1 })

    await eventually(async () => expect(creditResponses.length).equal(0), 10000)
    await connection.close()
  }).timeout(10000)

  it("ask for credit after subscribing to the next message with wrong subscriptionId", async () => {
    creditResponses.length = 0
    const connection = await connect({
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
      frameMax: 0, // not used
      heartbeat: 0, // not used
      listeners: {
        metadata_update: (_data) => console.info("Subscribe server error"),
        credit: (data) => creditResponses.push(data),
      },
    })
    await connection.subscribe({
      subscriptionId: 1,
      stream: streamName,
      offset: Offset.next(),
      credit: 1,
    })

    await connection.askForCredit({ subscriptionId: 100, credit: 1 })

    await eventually(async () => expect(creditResponses.length).greaterThanOrEqual(1), 10000)
    await connection.close()
  }).timeout(10000)
})
