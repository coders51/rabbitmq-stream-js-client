import { Logger } from "winston"
import { HeartbeatRequest } from "./requests/heartbeat_request"

export interface HeartbeatConnection {
  close(): Promise<void>
  send(data: Buffer): Promise<void>
}

export class Heartbeat {
  private MAX_HEARTBEATS_MISSED = 2
  private interval: number = 0
  private connection: HeartbeatConnection
  private logger: Logger
  private lastMessageReceived = new Date()
  private lastMessageSent = new Date()
  private idleCounter: number = 0

  constructor(interval: number, connection: HeartbeatConnection, logger: Logger) {
    this.interval = interval * 1000
    this.connection = connection
    this.logger = logger

    setTimeout(async () => {
      await this.heartbeat()
    }, this.interval)
  }

  reportLastMessageReceived() {
    this.lastMessageReceived = new Date()
    this.idleCounter = 0
  }

  reportLastMessageSent() {
    this.lastMessageSent = new Date()
    this.idleCounter = 0
  }

  private async heartbeat() {
    const now = new Date().getTime()
    const lastMessageSent = this.lastMessageSent.getTime()
    const noMessagesSentFor = Math.abs(now - lastMessageSent)
    this.logger.debug(`No messages sent for the last ${noMessagesSentFor} ms and the interval is ${this.interval}`)
    if (noMessagesSentFor >= this.interval) {
      await this.sendHeartbeat()
      this.reportLastMessageSent()
    }
    await this.idleDetection()

    setTimeout(async () => {
      await this.heartbeat()
    }, this.interval)
  }

  private async sendHeartbeat() {
    this.logger.debug("Sending heartbeat")
    const request = new HeartbeatRequest()
    const body = request.toBuffer()
    await this.connection.send(body)
  }

  private async idleDetection() {
    const lastMessageReceived = this.lastMessageReceived.getTime()
    const noMessagesReceivedFor = Math.abs(new Date().getTime() - lastMessageReceived)
    this.logger.debug(
      `No messages received for the last ${noMessagesReceivedFor} ms and the interval is ${this.interval}`
    )
    const lastMessageSent = this.lastMessageSent.getTime()
    const noMessagesSentFor = Math.abs(new Date().getTime() - lastMessageSent)
    this.logger.debug(`No messages sent for the last ${noMessagesSentFor} ms and the interval is ${this.interval}`)
    if (noMessagesReceivedFor > this.interval && noMessagesSentFor > this.interval) {
      this.lastMessageReceived = new Date()
      this.lastMessageSent = new Date()
      this.idleCounter++
      this.logger && this.logger.debug(`Heartbeat missed! counter: ${this.idleCounter}`)
    }
    if (this.idleCounter === this.MAX_HEARTBEATS_MISSED) {
      this.connection && (await this.connection.close())
    }
  }
}
