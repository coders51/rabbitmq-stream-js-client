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
