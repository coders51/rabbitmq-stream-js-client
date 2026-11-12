import { expect } from "chai"
import { Connection } from "../../src/connection"
import { ConnectionPool } from "../../src/connection_pool"
import { StreamConsumer } from "../../src/consumer"
import { Message } from "../../src/publisher"
import { Offset } from "../../src/requests/subscribe_request"

describe("StreamConsumer active state", () => {
  let storedOffsets: bigint[]
  let released: number

  const createConsumer = (singleActive: boolean, handle: (message: Message) => void = () => undefined) => {
    const connection = {
      connectionId: "connection-1",
      incrRefCount: () => undefined,
      freeConsumerId: () => undefined,
      storeOffset: ({ offsetValue }: { offsetValue: bigint }) => {
        storedOffsets.push(offsetValue)
        return Promise.resolve()
      },
    } as unknown as Connection
    const pool = {
      releaseConnection: () => {
        released++
        return Promise.resolve()
      },
    } as unknown as ConnectionPool
    return new StreamConsumer(pool, handle, {
      connection,
      stream: "s",
      consumerId: 1,
      consumerRef: "ref",
      offset: Offset.first(),
      singleActive,
    })
  }

  const message = (offset: bigint) => ({ offset, content: Buffer.from("m") }) as Message

  beforeEach(() => {
    storedOffsets = []
    released = 0
  })

  describe("a consumer that is not single active", () => {
    it("is active, with a signal that has not aborted", () => {
      const consumer = createConsumer(false)

      expect(consumer.isActive()).eql(true)
      expect(consumer.activeSignal.aborted).eql(false)
    })

    it("handles messages and stores offsets", async () => {
      const handled: bigint[] = []
      const consumer = createConsumer(false, (m) => {
        handled.push(m.offset!)
      })

      await consumer.handle(message(1n))
      await consumer.storeOffset(1n)

      expect(handled).eql([1n])
      expect(storedOffsets).eql([1n])
    })
  })

  describe("a single active consumer", () => {
    it("is not active until the broker activates it, with a signal that is already aborted", () => {
      const consumer = createConsumer(true)

      expect(consumer.isActive()).eql(false)
      expect(consumer.activeSignal.aborted).eql(true)
    })

    it("becomes active when the broker says so", async () => {
      const handled: bigint[] = []
      const consumer = createConsumer(true, (m) => {
        handled.push(m.offset!)
      })

      consumer.setActive(true)
      await consumer.handle(message(1n))
      await consumer.storeOffset(1n)

      expect(consumer.isActive()).eql(true)
      expect(consumer.activeSignal.aborted).eql(false)
      expect(handled).eql([1n])
      expect(storedOffsets).eql([1n])
    })
  })

  describe("losing and regaining the active state", () => {
    it("aborts the signal that was read while the consumer was active", () => {
      const consumer = createConsumer(false)
      const signal = consumer.activeSignal

      consumer.setActive(false)

      expect(signal.aborted).eql(true)
      expect(consumer.isActive()).eql(false)
    })

    it("does not stop the consumer from handling messages or storing offsets: that is up to the handler", async () => {
      const handled: bigint[] = []
      const consumer = createConsumer(false, (m) => {
        handled.push(m.offset!)
      })
      consumer.setActive(false)

      await consumer.handle(message(1n))
      await consumer.storeOffset(1n)

      expect(handled).eql([1n])
      expect(storedOffsets).eql([1n])
    })

    it("gives a new signal when the consumer is active again, and the old one stays aborted", () => {
      const consumer = createConsumer(false)
      const first = consumer.activeSignal
      consumer.setActive(false)

      consumer.setActive(true)

      expect(consumer.isActive()).eql(true)
      expect(first.aborted).eql(true)
      expect(consumer.activeSignal).not.equal(first)
      expect(consumer.activeSignal.aborted).eql(false)
    })

    it("does nothing when the state does not change", () => {
      const consumer = createConsumer(false)
      const signal = consumer.activeSignal

      consumer.setActive(true)

      expect(consumer.activeSignal).equal(signal)
      expect(signal.aborted).eql(false)
    })
  })

  describe("closing", () => {
    it("makes the consumer inactive and aborts its signal", async () => {
      const consumer = createConsumer(false)
      const signal = consumer.activeSignal

      await consumer.close()

      expect(consumer.isActive()).eql(false)
      expect(signal.aborted).eql(true)
      expect(released).eql(1)
    })

    it("cannot be undone by the broker", async () => {
      const consumer = createConsumer(true)
      await consumer.close()

      consumer.setActive(true)

      expect(consumer.isActive()).eql(false)
      expect(consumer.activeSignal.aborted).eql(true)
    })

    it("also applies to an automatic close", async () => {
      const consumer = createConsumer(false)
      const signal = consumer.activeSignal

      await consumer.automaticClose()

      expect(consumer.isActive()).eql(false)
      expect(signal.aborted).eql(true)
    })
  })
})
