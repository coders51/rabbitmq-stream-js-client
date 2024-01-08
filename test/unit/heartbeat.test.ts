import { expect } from "chai"
import { Heartbeat, HeartbeatConnection } from "../../src/heartbeat"
import { NullLogger } from "../../src/logger"
import { Request } from "../../src/requests/request"
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
})
