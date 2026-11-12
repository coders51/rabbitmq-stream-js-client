import { expect } from "chai"
import { Heartbeat, HeartbeatConnection } from "../../src/heartbeat"
import { NullLogger } from "../../src/logger"
import { Request } from "../../src/requests/request"
import { SocketNotOpenError } from "../../src/socket_not_open_error"
import { eventually, expectToThrowAsync, wait } from "../support/util"

class ConnectionMock implements HeartbeatConnection {
  private sendCount = 0

  close(): Promise<void> {
    throw new Error("Method not implemented.")
  }

  send(_cmd: Request): Promise<void> {
    this.sendCount++
    return Promise.resolve()
  }

  getSendCount() {
    return this.sendCount
  }
}

describe("heartbeat", () => {
  const logger = new NullLogger()

  it("sent heartbeat every seconds", async () => {
    const connectionMock = new ConnectionMock()
    const hb = new Heartbeat(connectionMock, logger)

    hb.start(1)

    await eventually(async () => expect(connectionMock.getSendCount()).eq(4), 6000)
    hb.stop()
  }).timeout(10000)

  it("stop check", async () => {
    const connectionMock = new ConnectionMock()
    const hb = new Heartbeat(connectionMock, logger)
    hb.start(1)
    hb.stop()

    await wait(4000)
    expect(connectionMock.getSendCount()).lessThanOrEqual(1)
  }).timeout(10000)

  it("stop current timeout so we could exit immediately", () => {
    const connectionMock = new ConnectionMock()
    const hb = new Heartbeat(connectionMock, logger)
    hb.start(200)

    hb.stop()
  })

  it("start two times same object raise exception", async () => {
    const connectionMock = new ConnectionMock()
    const hb = new Heartbeat(connectionMock, logger)
    hb.start(1)

    await expectToThrowAsync(async () => hb.start(1), Error, "HeartBeat already started")
    hb.stop()
  })

  it("stops, without rescheduling, when the socket is not open, and can be started again", async () => {
    const connection = new ConnectionMock()
    let sends = 0
    connection.send = () => {
      sends++
      return Promise.reject(new SocketNotOpenError("c1"))
    }
    const unhandled: unknown[] = []
    const onUnhandled = (e: unknown) => unhandled.push(e)
    process.on("unhandledRejection", onUnhandled)
    const hb = new Heartbeat(connection, logger)
    hb.start(1)

    await wait(2500)
    process.off("unhandledRejection", onUnhandled)

    expect(unhandled).eql([])
    expect(sends).eql(1)
    expect(hb.started).eql(false)
    hb.start(1)
    expect(hb.started).eql(true)
    hb.stop()
  }).timeout(10000)

  it("does not hide other send errors", async () => {
    const connection = new ConnectionMock()
    connection.send = () => Promise.reject(new Error("write failed"))
    const unhandled: unknown[] = []
    const onUnhandled = (e: unknown) => unhandled.push(e)
    process.on("unhandledRejection", onUnhandled)
    const hb = new Heartbeat(connection, logger)
    hb.start(1)

    await wait(1500)
    hb.stop()
    process.off("unhandledRejection", onUnhandled)

    expect(unhandled.length).to.be.greaterThan(0)
    expect(unhandled.every((e) => (e as Error).message === "write failed")).eql(true)
  }).timeout(10000)
})
