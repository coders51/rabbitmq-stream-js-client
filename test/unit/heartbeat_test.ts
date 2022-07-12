import { expect } from "chai"
import { Heartbeat, HeartbeatConnection } from "../../src/heartbeat"
import { createConsoleLog } from "../../src/util"
import { eventually } from "../support/util"

class ConnectionMock implements HeartbeatConnection {
  private sendCount = 0
  close(): Promise<void> {
    throw new Error("Method not implemented.")
  }
  send(_data: Buffer): Promise<void> {
    this.sendCount++
    return new Promise((res, _rej) => {
      return res()
    })
  }

  getSendCount() {
    return this.sendCount
  }
}

describe("heartbeat", () => {
  it("sent heartbeat every seconds", async () => {
    const connectionMock = new ConnectionMock()
    new Heartbeat(1, connectionMock, createConsoleLog())

    await eventually(async () => {
      expect(connectionMock.getSendCount()).eq(4)
    }, 5000)
  }).timeout(5000)
})
