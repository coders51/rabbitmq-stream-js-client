import { expect } from "chai"
import { Client, Offset, SocketNotOpenError } from "../../src"
import { Consumer } from "../../src/consumer"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"

describe("socket not open", () => {
  const rabbit = new Rabbit(username, password)
  let client: Client
  let streamName: string

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
    } catch (e) {}
  })

  const destroySocketOf = (consumer: Consumer) =>
    (consumer as unknown as { connection: { socket: { destroy: () => void } } }).connection.socket.destroy()

  it("storeOffset rejects with SocketNotOpenError once the connection is lost", async () => {
    const consumer = await client.declareConsumer(
      { stream: streamName, offset: Offset.first(), consumerRef: "ref" },
      () => undefined
    )
    destroySocketOf(consumer)

    let error: unknown
    await eventually(async () => {
      error = await consumer.storeOffset(1n).then(
        () => undefined,
        (e) => e
      )
      expect(error).instanceOf(SocketNotOpenError)
    })
  }).timeout(10000)

  it("a delivery handled after the connection is lost does not reject", async () => {
    const consumer = await client.declareConsumer({ stream: streamName, offset: Offset.first() }, () => undefined)
    destroySocketOf(consumer)
    const internals = client as unknown as {
      handleDelivery: (d: unknown) => Promise<void>
      consumers: Map<string, unknown>
    }
    const [consumerId] = [...internals.consumers.keys()]

    await internals.handleDelivery({
      messages: [],
      subscriptionId: 0,
      consumerId,
      messageFilteringSupported: false,
    })
  }).timeout(10000)
})
