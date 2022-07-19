import { expect } from "chai"
import { Heartbeat, HeartbeatConnection } from "../../src/heartbeat"
import { Request } from "../../src/requests/request"
import { createConsoleLog } from "../../src/util"
import { eventually } from "../support/util"

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
  it("sent heartbeat every seconds", async () => {
    const connectionMock = new ConnectionMock()
    new Heartbeat(connectionMock, createConsoleLog()).start(1)

    await eventually(async () => {
      expect(connectionMock.getSendCount()).eq(4)
    }, 5000)
  }).timeout(5000)
})
