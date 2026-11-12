import { expect } from "chai"
import { Client, Offset } from "../../src"
import { Consumer } from "../../src/consumer"
import { Message } from "../../src/publisher"
import { createClient, createStreamName } from "../support/fake_data"
import { Rabbit } from "../support/rabbit"
import { eventually, password, username, waitSleeping } from "../support/util"
import { randomUUID } from "crypto"

describe("consumer active state", () => {
  const rabbit = new Rabbit(username, password)
  let client: Client
  let streamName: string

  beforeEach(async () => {
    client = await createClient(username, password)
    streamName = createStreamName()
  })

  afterEach(async () => {
    try {
      await client.close()
      await rabbit.deleteStream(streamName)
      await rabbit.deleteSuperStream(streamName)
      await rabbit.closeAllConnections()
      await rabbit.deleteAllQueues({ match: /my-stream-/ })
    } catch (e) {}
  })

  const connectionOf = (consumer: Consumer) =>
    (consumer as unknown as { connection: { connectionId: string } }).connection

  describe("a consumer that is not single active", () => {
    beforeEach(async () => {
      await rabbit.createStream(streamName)
    })

    it("is active, and inactive once it is closed", async () => {
      const consumer = await client.declareConsumer({ stream: streamName, offset: Offset.first() }, () => undefined)
      const signal = consumer.deactivatedSignal
      expect(consumer.isActive()).eql(true)
      expect(signal.aborted).eql(false)

      await consumer.close()

      expect(consumer.isActive()).eql(false)
      expect(signal.aborted).eql(true)
    }).timeout(10000)

    it("is inactive when its connection is lost, and active again, with a new signal, after the restart", async () => {
      const consumer = await client.declareConsumer({ stream: streamName, offset: Offset.first() }, () => undefined)
      const signal = consumer.deactivatedSignal

      ;(connectionOf(consumer) as unknown as { socket: { destroy: () => void } }).socket.destroy()

      await eventually(() => {
        expect(consumer.isActive()).eql(false)
        expect(signal.aborted).eql(true)
      })
      await client.restart()
      await eventually(() => {
        expect(consumer.isActive()).eql(true)
      })
      expect(consumer.deactivatedSignal).not.equal(signal)
      expect(consumer.deactivatedSignal.aborted).eql(false)
    }).timeout(20000)

    it("is told through its signal when client.restart() replaces its healthy connection", async () => {
      const consumer = await client.declareConsumer({ stream: streamName, offset: Offset.first() }, () => undefined)
      const signal = consumer.deactivatedSignal
      expect(consumer.isActive()).eql(true)

      await client.restart()

      expect(signal.aborted).eql(true)
      await eventually(() => {
        expect(consumer.isActive()).eql(true)
      })
      expect(consumer.deactivatedSignal).not.equal(signal)
      expect(consumer.deactivatedSignal.aborted).eql(false)
    }).timeout(20000)

    it("is already active, with its new signal, when the resubscription is sent, so deliveries decoded with the subscribe reply see a live signal", async () => {
      const consumer = await client.declareConsumer({ stream: streamName, offset: Offset.first() }, () => undefined)
      const internals = client as unknown as { declareConsumerOnConnection: (...args: unknown[]) => Promise<void> }
      const original = internals.declareConsumerOnConnection.bind(client)
      const seenAtSubscribe: boolean[] = []
      internals.declareConsumerOnConnection = (...args: unknown[]) => {
        seenAtSubscribe.push(consumer.isActive() && !consumer.deactivatedSignal.aborted)
        return original(...args)
      }

      await client.restart()

      expect(seenAtSubscribe).eql([true])
    }).timeout(20000)
  })

  describe("single active consumers", () => {
    it("are active when the broker makes them the active consumer, and not while another one is", async () => {
      await rabbit.createStream(streamName)
      const consumerRef = randomUUID()
      const declare = () =>
        client.declareConsumer(
          { stream: streamName, offset: Offset.first(), consumerRef, singleActive: true },
          () => undefined
        )

      const first = await declare()
      await eventually(() => {
        expect(first.isActive()).eql(true)
      })
      const second = await declare()
      await waitSleeping(500)

      expect(first.isActive()).eql(true)
      expect(second.isActive()).eql(false)
      expect(second.deactivatedSignal.aborted).eql(true)
      await client.closeConsumer(first.extendedId)
      await eventually(() => {
        expect(second.isActive()).eql(true)
      })
      expect(second.deactivatedSignal.aborted).eql(false)
    }).timeout(20000)

    it("on a super stream: the consumer that the broker gives the partition away from is told, and nothing is delivered twice", async () => {
      const noOfPartitions = await rabbit.createSuperStream(streamName, 2)
      const consumerRef = randomUUID()
      const receivedBy = new Map<string, string[]>()
      const consumersOf = new Map<string, Consumer[]>()
      const declare = (name: string) =>
        client.declareSuperStreamConsumer(
          { superStream: streamName, consumerRef },
          (message: Message, consumer: Consumer) => {
            const key = `${name}:${consumer.streamName}`
            receivedBy.set(key, [...(receivedBy.get(key) ?? []), message.content.toString()])
          }
        )

      await declare("a")
      await eventually(() => {
        expect(client.getConsumers().filter(({ consumer }) => consumer.isActive())).to.have.length(noOfPartitions)
      })
      const before = client.getConsumers().map(({ consumer }) => consumer)
      const signals = new Map(before.map((consumer) => [consumer, consumer.deactivatedSignal]))
      await declare("b")

      await eventually(() => {
        for (const { consumer } of client.getConsumers()) {
          consumersOf.set(consumer.streamName, [...(consumersOf.get(consumer.streamName) ?? []), consumer])
        }
        const all = client.getConsumers().map(({ consumer }) => consumer)
        expect(all).to.have.length(noOfPartitions * 2)
        for (const partition of new Set(all.map((c) => c.streamName))) {
          expect(all.filter((c) => c.streamName === partition && c.isActive())).to.have.length(1)
        }
      })

      await eventually(() => {
        expect(before.some((consumer) => !consumer.isActive())).eql(true)
      })
      for (const consumer of before) {
        if (!consumer.isActive()) {
          expect(signals.get(consumer)?.aborted).eql(true)
        }
      }

      const publisher = await client.declareSuperStreamPublisher({ superStream: streamName }, (_c, o) =>
        o.applicationProperties?.["k"]?.toString()
      )
      for (let k = 0; k < noOfPartitions; k++) {
        for (let n = 0; n < 5; n++) {
          await publisher.send(Buffer.from(`${k}-${n}`), { applicationProperties: { k: `${k}` } })
        }
      }
      await eventually(() => {
        const total = [...receivedBy.values()].reduce((sum, messages) => sum + messages.length, 0)
        expect(total).to.equal(noOfPartitions * 5)
      })
      await waitSleeping(300)
      const all = [...receivedBy.values()].flat()
      expect(new Set(all).size).to.equal(all.length)
      expect(all).to.have.length(noOfPartitions * 5)
    }).timeout(30000)
  })
})
