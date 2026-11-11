import { use as chaiUse, expect, spy } from "chai"
import spies from "chai-spies"
import { randomUUID } from "crypto"
import { Connection } from "../../src/connection"
import { ConnectionPool } from "../../src/connection_pool"
import { StreamConsumer } from "../../src/consumer"
import { NullLogger } from "../../src/logger"
import { StreamPublisher } from "../../src/publisher"
import { Offset } from "../../src/requests/subscribe_request"
import { getTestNodesFromEnv, password, username } from "../support/util"
chaiUse(spies)

describe("Close failures", () => {
  const logger = new NullLogger()
  let pool: ConnectionPool
  let connection: Connection
  let sandbox: ChaiSpies.Sandbox

  beforeEach(async () => {
    const [firstNode] = getTestNodesFromEnv()
    pool = new ConnectionPool(logger)
    connection = await Connection.connect(
      {
        hostname: firstNode.host,
        port: firstNode.port,
        username,
        password,
        vhost: "/",
        frameMax: 0,
        heartbeat: 0,
        connectionId: randomUUID(),
      },
      logger
    )
    sandbox = spy.sandbox()
  })

  afterEach(async () => {
    sandbox.restore()
    if (connection.ready) await connection.close().catch(() => undefined)
  })

  const createPublisher = () =>
    new StreamPublisher(pool, { connection, stream: "s", publisherId: 0, maxFrameSize: 0, logger }, 0n)

  const createConsumer = () =>
    new StreamConsumer(pool, () => undefined, { connection, stream: "s", consumerId: 0, offset: Offset.first() })

  describe("close", () => {
    it("publisher releases the connection even if flush fails", async () => {
      const publisher = createPublisher()
      sandbox.on(publisher, "flush", async () => {
        throw new Error("flush failed")
      })
      sandbox.on(connection, "close")

      await publisher.close().catch(() => undefined)
      await publisher.close().catch(() => undefined)

      expect(publisher.closed).eql(true)
      expect(connection.refCount).eql(0)
      expect(connection.close).to.have.been.called.once
    })

    it("publisher called twice releases the connection only once", async () => {
      connection.incrRefCount()
      const publisher = createPublisher()

      await publisher.close()
      await publisher.close()

      expect(connection.refCount).eql(1)
    })

    it("consumer releases the connection even if connection close fails", async () => {
      sandbox.on(connection, "close", async () => {
        throw new Error("close failed")
      })
      const consumer = createConsumer()

      await consumer.close().catch(() => undefined)
      await consumer.close().catch(() => undefined)

      expect(connection.refCount).eql(0)
    })
  })

  describe("close with a close handler", () => {
    it("goes through the handler, which finishes by releasing the consumer locally", async () => {
      connection.incrRefCount()
      const consumer = createConsumer()
      const handler = spy(async () => {
        await consumer.closeInternal()
      })
      consumer.setCloseHandler(handler)

      await consumer.close()

      expect(handler).to.have.been.called.once
      expect(connection.refCount).eql(1)
    })

    it("concurrent calls share one handler call", async () => {
      const consumer = createConsumer()
      const handler = spy(async () => {
        await new Promise((resolve) => setTimeout(resolve, 10))
        await consumer.closeInternal()
      })
      consumer.setCloseHandler(handler)

      await Promise.all([consumer.close(), consumer.close()])
      await consumer.close()

      expect(handler).to.have.been.called.once
    })

    it("a call that joins a close in progress waits for it to finish, even once the consumer is marked closed", async () => {
      const consumer = createConsumer()
      let finishHandler!: () => void
      consumer.setCloseHandler(async () => {
        await consumer.closeInternal()
        // the client still has to remove the registry entry
        await new Promise<void>((resolve) => {
          finishHandler = resolve
        })
      })
      const first = consumer.close()
      await new Promise((resolve) => setTimeout(resolve, 20))

      let joined = false
      const second = consumer.close().then(() => {
        joined = true
      })
      await new Promise((resolve) => setTimeout(resolve, 20))
      expect(joined).eql(false)

      finishHandler()
      await Promise.all([first, second])
      expect(joined).eql(true)
    })

    it("a failed close can be repeated", async () => {
      const consumer = createConsumer()
      let attempts = 0
      consumer.setCloseHandler(async () => {
        if (attempts++ === 0) throw new Error("unsubscribe failed")
        await consumer.closeInternal()
      })

      await consumer.close().then(
        () => expect.fail("expected the close to fail"),
        (e: Error) => expect(e.message).eql("unsubscribe failed")
      )
      await consumer.close()

      expect(attempts).eql(2)
      expect(connection.refCount).eql(0)
    })
  })

  describe("automaticClose", () => {
    it("publisher releases the connection even if flush fails", async () => {
      const publisher = createPublisher()
      sandbox.on(publisher, "flush", async () => {
        throw new Error("flush failed")
      })
      sandbox.on(connection, "close")

      await publisher.automaticClose().catch(() => undefined)
      await publisher.automaticClose().catch(() => undefined)

      expect(publisher.closed).eql(true)
      expect(connection.refCount).eql(0)
      expect(connection.close).to.have.been.called.once
    })

    it("publisher called twice releases the connection only once", async () => {
      connection.incrRefCount()
      const publisher = createPublisher()

      await publisher.automaticClose()
      await publisher.automaticClose()

      expect(connection.refCount).eql(1)
    })

    it("consumer releases the connection even if connection close fails", async () => {
      sandbox.on(connection, "close", async () => {
        throw new Error("close failed")
      })
      const consumer = createConsumer()

      await consumer.automaticClose().catch(() => undefined)
      await consumer.automaticClose().catch(() => undefined)

      expect(connection.refCount).eql(0)
    })
  })
})
