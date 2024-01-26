import { expect, spy } from "chai"
import { Client } from "../../src"
import { createClient } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { username, password, eventually, always } from "../support/util"
import { randomUUID } from "crypto"
import { Offset } from "../../src/requests/subscribe_request"
describe("connection closed callback", () => {
  let client: Client | undefined = undefined
  const rabbit = new Rabbit(username, password)
  let spySandbox: ChaiSpies.Sandbox | null = null
  let streamName: string = ""
  const publisherRef = "the-publisher"
  const consumerRef = "the-consumer"

  beforeEach(async () => {
    spySandbox = spy.sandbox()
    streamName = `my-stream-${randomUUID()}`
    await rabbit.createStream(streamName)
  })

  afterEach(async () => {
    spySandbox?.restore()
    try {
      await client?.close()
      await rabbit.deleteStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}

    try {
      await rabbit.closeAllConnections()
    } catch (e) {}
  })

  it("is invoked after close operation", async () => {
    const listener = (_hasError: boolean) => {
      return
    }
    const listenerSpy = spy(listener)
    client = await createClient(username, password, { connection_closed: listenerSpy })

    await client.close()

    await eventually(() => {
      expect(listenerSpy).to.have.been.called
    }, 1000)
  }).timeout(5000)

  it("is invoked only on locator socket event", async () => {
    const listener = (_hasError: boolean) => {
      return
    }
    const listenerSpy = spy(listener)
    client = await createClient(username, password, { connection_closed: listenerSpy })
    await client.declarePublisher({ stream: streamName, publisherRef })
    await client.declareConsumer({ stream: streamName, consumerRef, offset: Offset.first() }, (_msg) => {
      return
    })

    await client.close()

    await always(() => {
      expect(listenerSpy).to.have.been.called.lt(2)
    }, 1000)
  }).timeout(5000)

  it("if specified, is called also on publisher and consumer socket events", async () => {
    const listener = (_hasError: boolean) => {
      return
    }
    const listenerSpy = spy(listener)
    client = await createClient(username, password, { connection_closed: listenerSpy })
    await client.declarePublisher({ stream: streamName, publisherRef, connectionClosedListener: listenerSpy })
    await client.declareConsumer(
      { stream: streamName, consumerRef, offset: Offset.first(), connectionClosedListener: listenerSpy },
      (_msg) => {
        return
      }
    )

    await client.close()

    await eventually(() => {
      expect(listenerSpy).to.have.been.called.exactly(3)
    }, 1000)
  }).timeout(5000)

  it("different callbacks for client, consumer and publisher are all called when connections close", async () => {
    const instListener = () => {
      return (_hasError: boolean) => {
        return
      }
    }
    const listenerClientSpy = spy(instListener())
    const listenerConsumerSpy = spy(instListener())
    const listenerPublisherSpy = spy(instListener())
    client = await createClient(username, password, { connection_closed: listenerClientSpy })
    await client.declarePublisher({ stream: streamName, publisherRef, connectionClosedListener: listenerConsumerSpy })
    await client.declareConsumer(
      { stream: streamName, consumerRef, offset: Offset.first(), connectionClosedListener: listenerPublisherSpy },
      (_msg) => {
        return
      }
    )

    await client.close()

    await eventually(() => {
      expect(listenerClientSpy).to.have.been.called.exactly(1)
    }, 1000)
    await eventually(() => {
      expect(listenerConsumerSpy).to.have.been.called.exactly(1)
    }, 1000)
    await eventually(() => {
      expect(listenerPublisherSpy).to.have.been.called.exactly(1)
    }, 1000)
  }).timeout(5000)
})
