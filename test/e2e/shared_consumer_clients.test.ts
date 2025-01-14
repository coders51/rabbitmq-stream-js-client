import { expect, spy } from "chai"
import { Client, Offset, Publisher } from "../../src"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username } from "../support/util"
import { Message } from "../../src/publisher"
import { randomUUID } from "crypto"

describe("consume messages through multiple consumers", () => {
  const rabbit = new Rabbit(username, password)
  const testStreamName = createStreamName()
  let client: Client
  let publisher: Publisher

  beforeEach(async () => {
    await rabbit.createStream(testStreamName)
    client = await createClient(username, password)
    publisher = await client.declarePublisher({ stream: testStreamName })
  })

  afterEach(async () => {
    await client.close()
    await rabbit.deleteStream(testStreamName)
  })

  it("single publisher, all consumer callbacks are called", async () => {
    const howManyConsumers = 3
    const howManyPublished = 10
    const spiedCallbacks: ChaiSpies.SpyFunc1Proxy<Message, void>[] = []
    for (let i = 0; i < howManyConsumers; i++) {
      const cb = (_msg: Message) => {
        return
      }
      const spied = spy(cb)

      await client.declareConsumer({ stream: testStreamName, offset: Offset.first() }, spied)
      spiedCallbacks.push(spied)
    }

    for (let index = 0; index < howManyPublished; index++) {
      await publisher.send(Buffer.from(`test${randomUUID()}`))
    }

    await eventually(async () => {
      spiedCallbacks.forEach((cb) => {
        expect(cb).to.have.been.called.exactly(howManyPublished)
      })
    }, 5000)
  }).timeout(6000)

  it("multiple publishers, all consumer callbacks are called", async () => {
    const howManyConsumers = 3
    const howManyPublished = 10
    const publishers = [
      publisher,
      await client.declarePublisher({ stream: testStreamName }),
      await client.declarePublisher({ stream: testStreamName }),
    ]
    const spiedCallbacks: ChaiSpies.SpyFunc1Proxy<Message, void>[] = []
    for (let i = 0; i < howManyConsumers; i++) {
      const cb = (_msg: Message) => {
        return
      }
      const spied = spy(cb)

      await client.declareConsumer({ stream: testStreamName, offset: Offset.first() }, spied)
      spiedCallbacks.push(spied)
    }

    for (let index = 0; index < howManyPublished; index++) {
      for (const p of publishers) {
        await p.send(Buffer.from(`test${randomUUID()}`))
      }
    }

    await eventually(async () => {
      spiedCallbacks.forEach((cb) => {
        expect(cb).to.have.been.called.exactly(howManyPublished * publishers.length)
      })
    }, 5000)
  }).timeout(6000)
})
