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
      await rabbit.deleteStream(streamName)
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
      await client?.close()
      await rabbit.closeAllConnections()
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

  it("closed_connection listener is not invoked by the client", async () => {
    const listener = (_hasError: boolean) => {
      return
    }
    const clientListenerSpy = spy(listener)
    client = await createClient(username, password, { connection_closed: clientListenerSpy })

    await client.close()

    await always(() => {
      expect(clientListenerSpy).to.have.been.called.exactly(0)
    })
  }).timeout(5000)

  it("closed_connection listener is not invoked by the deletePublisher", async () => {
    const listener = (_hasError: boolean) => {
      return
    }
    const publisherListenerSpy = spy(listener)
    client = await createClient(username, password, { connection_closed: publisherListenerSpy })
    const publisher = await client.declarePublisher({
      stream: streamName,
      publisherRef,
      connectionClosedListener: publisherListenerSpy,
    })

    await client.deletePublisher(publisher.extendedId)

    await always(() => {
      expect(publisherListenerSpy).to.have.been.called.exactly(0)
    })
  }).timeout(5000)

  it("closed_connection listener is not invoked by the deleteConsumer", async () => {
    const listener = (_hasError: boolean) => {
      return
    }
    const consumerListenerSpy = spy(listener)
    client = await createClient(username, password, { connection_closed: consumerListenerSpy })
    const consumer = await client.declareConsumer(
      { stream: streamName, consumerRef, offset: Offset.first(), connectionClosedListener: consumerListenerSpy },
      (_msg) => {
        return
      }
    )

    await client.closeConsumer(consumer.extendedId)

    await always(() => {
      expect(consumerListenerSpy).to.have.been.called.exactly(0)
    })
  }).timeout(5000)

  it("closed_connection listener is not invoked by the client even with multiple publishers and consumers", async () => {
    const listener = (_hasError: boolean) => {
      return
    }
    const clientListenerSpy = spy(listener)
    const publisherListenerSpy = spy(listener)
    const consumerListenerSpy = spy(listener)
    client = await createClient(username, password, { connection_closed: clientListenerSpy })
    await client.declarePublisher({
      stream: streamName,
      publisherRef,
      connectionClosedListener: publisherListenerSpy,
    })
    await client.declarePublisher({
      stream: streamName,
      publisherRef: `${publisherRef}-1`,
      connectionClosedListener: publisherListenerSpy,
    })
    await client.declareConsumer(
      { stream: streamName, consumerRef, offset: Offset.first(), connectionClosedListener: consumerListenerSpy },
      (_msg) => {
        return
      }
    )
    await client.declareConsumer(
      {
        stream: streamName,
        consumerRef: `${consumerRef}-1`,
        offset: Offset.first(),
        connectionClosedListener: consumerListenerSpy,
      },
      (_msg) => {
        return
      }
    )

    await client.close()

    await always(() => {
      expect(clientListenerSpy).to.have.been.called.exactly(0)
      expect(publisherListenerSpy).to.have.been.called.exactly(0)
      expect(consumerListenerSpy).to.have.been.called.exactly(0)
    })
  }).timeout(5000)

  it("closed_connection listener is invoked by the server if it closes the connection", async () => {
    const listener = (_hasError: boolean) => {
      return
    }
    const clientListenerSpy = spy(listener)
    const publisherListenerSpy = spy(listener)
    const consumerListenerSpy = spy(listener)
    client = await createClient(username, password, { connection_closed: clientListenerSpy })
    await client.declarePublisher({
      stream: streamName,
      publisherRef,
      connectionClosedListener: publisherListenerSpy,
    })
    await client.declareConsumer(
      { stream: streamName, consumerRef, offset: Offset.first(), connectionClosedListener: consumerListenerSpy },
      (_msg) => {
        return
      }
    )
    await sleep(5000)

    await rabbit.closeAllConnections()

    await eventually(() => {
      expect(clientListenerSpy).to.have.been.called.at.least(1)
      expect(publisherListenerSpy).to.have.been.called.at.least(1)
      expect(consumerListenerSpy).to.have.been.called.at.least(1)
    }, 5000)
    await always(() => {
      expect(clientListenerSpy).to.have.been.called.at.most(1)
      expect(publisherListenerSpy).to.have.been.called.at.most(1)
      expect(consumerListenerSpy).to.have.been.called.at.most(1)
    }, 5000)
  }).timeout(15000)

  it("closed_connection listener is not invoked on publishers and consumers if not explicitly set", async () => {
    let ctr = 0
    const listener = (_hasError: boolean) => {
      ctr++
      if (ctr > 1) throw new Error("")
      return
    }
    const clientListenerSpy = spy(listener)
    client = await createClient(username, password, { connection_closed: clientListenerSpy })
    await client.declarePublisher({
      stream: streamName,
      publisherRef,
    })
    await client.declareConsumer({ stream: streamName, consumerRef, offset: Offset.first() }, (_msg) => {
      return
    })
    await sleep(5000)

    await rabbit.closeAllConnections()

    await eventually(() => {
      expect(clientListenerSpy).to.have.been.called.at.least(1)
    }, 5000)
    await always(() => {
      expect(clientListenerSpy).to.have.been.called.at.most(1)
    }, 5000)
  }).timeout(15000)
})

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
