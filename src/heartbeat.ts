import { Logger } from "./logger"
import { HeartbeatRequest } from "./requests/heartbeat_request"
import { Request } from "./requests/request"

export interface HeartbeatConnection {
  send(cmd: Request): Promise<void>
  close(): Promise<void>
}

export class Heartbeat {
  private MAX_HEARTBEATS_MISSED = 2
  private interval: number = 0
  private lastMessageReceived = new Date()
  private lastMessageSent = new Date()
  private idleCounter: number = 0
  private timeout: NodeJS.Timeout | null = null
  private heartBeatStarted = false

  constructor(
    private readonly connection: HeartbeatConnection,
    private readonly logger: Logger
  ) {}

  start(secondsInterval: number) {
    if (this.heartBeatStarted) throw new Error("HeartBeat already started")
    if (secondsInterval <= 0) return
    this.interval = secondsInterval * 1000
    this.heartBeatStarted = true
    this.timeout = setTimeout(() => this.heartbeat(), this.interval)
  }

  stop() {
    // TODO -> Wait the cycle of heartbeat...
    if (this.timeout) {
      clearTimeout(this.timeout)
    }
    this.interval = 0
    this.heartBeatStarted = false
  }

  public get started() {
    return this.heartBeatStarted
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
    if (this.interval <= 0) return
    this.timeout = setTimeout(() => this.heartbeat(), this.interval)
  }

  private sendHeartbeat() {
    this.logger.debug("Sending heartbeat")
    // TODO -> raise and event instead of send data
    return this.connection.send(new HeartbeatRequest())
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
      // TODO -> raise an event instead of make the action
      await this.connection.close()
    }
  }
}
