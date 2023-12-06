import { inspect } from "util"
import { Metrics } from "./metrics"
import { Connection, DeclarePublisherParams, IProducer } from "rabbitmq-stream-js-client"
import { Logger } from "winston"

export class PerfTestProducer {
  private readonly metrics = new Metrics()
  private payload: Buffer
  private readonly maxChunkSize: number = 1000
  private ctr = 0
  private displayTimer: NodeJS.Timeout | null

  constructor(
    private readonly connection: Connection,
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
    const publisher = await this.connection.declarePublisher(this.publisherParams)
    publisher.on("publish_confirm", (err, confirmedIds) => {
      if (err) {
        console.log(err)
      }
      this.metrics.addCounter("confirmed", confirmedIds.length)
    })

    this.displayTimer = setInterval(() => {
      this.displayMetrics()
      this.metrics.setStart()
    }, 1000)

    await this.send(publisher)
  }

  private displayMetrics(stop: boolean = false) {
    const metrics = { ...this.metrics.getMetrics(), total: this.ctr }
    this.logger.info(`${new Date().toISOString()} - ${inspect(metrics)}`)
    if (stop && this.displayTimer) {
      clearInterval(this.displayTimer)
    }
  }

  private async send(publisher: IProducer) {
    while (this.maxMessages === -1 || this.ctr < this.maxMessages) {
      const nmsgs = this.maxMessages > 0 ? Math.min(this.maxChunkSize, this.maxMessages) : this.maxChunkSize
      for (let index = 0; index < nmsgs; index++) {
        await publisher.send(this.payload, {})
      }
      this.ctr = this.ctr + nmsgs
      this.metrics.addCounter("published", nmsgs)
    }
  }

  public getDisplayTimer() {
    return this.displayTimer
  }
}
