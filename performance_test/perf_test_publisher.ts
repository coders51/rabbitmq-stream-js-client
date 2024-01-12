import { inspect } from "util"
import { Metrics } from "./metrics"
import { Client, DeclarePublisherParams, Publisher } from "rabbitmq-stream-js-client"
import { Logger } from "winston"

export class PerfTestPublisher {
  private readonly metrics = new Metrics()
  private payload: Buffer
  private readonly maxChunkSize: number = 1000
  private ctr = 0
  private displayTimer: NodeJS.Timeout | null

  constructor(
    private readonly client: Client,
    private readonly logger: Logger,
    private readonly maxMessages: number,
    private readonly publisherParams: DeclarePublisherParams,

    byteLength: number = 10
  ) {
    this.payload = Buffer.alloc(byteLength, Math.random().toString())
    this.metrics.setStart()
    this.displayTimer = null
  }

  public async cycle() {
    const publisher = await this.client.declarePublisher(this.publisherParams)
    publisher.on("publish_confirm", (err, confirmedIds) => {
      if (err) {
        this.logger.error(err)
      }
      this.metrics.addCounter("confirmed", confirmedIds.length)
    })

    this.displayTimer = setInterval(() => {
      this.displayMetrics()
      this.metrics.setStart()
    }, 500)

    await this.send(publisher)

    return true
  }

  private displayMetrics(stop: boolean = false) {
    const metrics = { ...this.metrics.getMetrics(), total: this.ctr }
    this.logger.info(`${inspect(metrics)}`)
    if (stop && this.displayTimer) {
      clearInterval(this.displayTimer)
    }
  }

  private async send(publisher: Publisher) {
    while (this.maxMessages === -1 || this.ctr < this.maxMessages) {
      const messageQuantity = this.maxMessages > 0 ? Math.min(this.maxChunkSize, this.maxMessages) : this.maxChunkSize
      for (let index = 0; index < messageQuantity; index++) {
        await publisher.send(this.payload, {})
      }
      this.ctr = this.ctr + messageQuantity
      this.metrics.addCounter("published", messageQuantity)
    }
    this.displayMetrics(true)
  }

  public getDisplayTimer() {
    return this.displayTimer
  }
}
