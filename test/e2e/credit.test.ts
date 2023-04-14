import { expect } from "chai"
import { Connection, connect } from "../../src"
import { CreditResponse } from "../../src/responses/credit_response"
import { Rabbit } from "../support/rabbit"
import { eventually } from "../support/util"
import { Offset } from "../../src/requests/subscribe_request"
import { Message } from "../../src/producer"

describe("update the metadata from the server", () => {
  const rabbit = new Rabbit()
  const streamName = "test-stream"
  const creditResponses: CreditResponse[] = []
  let connection: Connection

  beforeEach(async () => {
    connection = await connect({
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
  })
  beforeEach(async () => {
    try {
      await rabbit.deleteStream(streamName)
    } catch (error) {}
  })
  beforeEach(() => rabbit.createStream(streamName))

  afterEach(() => connection.close())

  it("ask for credit after subscribing to the next message", async () => {
    creditResponses.length = 0
    await connection.subscribe({
      subscriptionId: 1,
      stream: streamName,
      offset: Offset.next(),
      credit: 1,
    })

    await connection.askForCredit({ subscriptionId: 1, credit: 1 })

    await eventually(async () => expect(creditResponses.length).equal(0), 10000)
  }).timeout(10000)

  it("ask for credit after subscribing to the next message with wrong subscriptionId", async () => {
    creditResponses.length = 0
    await connection.subscribe({
      subscriptionId: 1,
      stream: streamName,
      offset: Offset.next(),
      credit: 1,
    })

    await connection.askForCredit({ subscriptionId: 100, credit: 1 })

    await eventually(async () => expect(creditResponses.length).greaterThanOrEqual(1), 10000)
  }).timeout(10000)

  it(`after declaring a consumer with initialCredit 10, and consuming 2 messages 
    the consumer should still have 10 credits`, async () => {
    const receivedMessages: Buffer[] = []
    const howMany = 2
    const messages = Array.from(Array(howMany).keys()).map((_) => Buffer.from("hello"))
    const publisher = await connection.declarePublisher({ stream: streamName })
    for (const m of messages) {
      await publisher.send(m)
    }

    await connection.declareConsumer({ stream: streamName, offset: Offset.first() }, (message: Message) =>
      receivedMessages.push(message.content)
    )

    await eventually(async () => expect(await rabbit.returnSingleConsumerCredits()).eql(10), 5000)
    await eventually(() => expect(receivedMessages).eql(messages), 1500)
  }).timeout(10000)
})
