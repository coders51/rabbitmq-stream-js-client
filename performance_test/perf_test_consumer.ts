import { inspect } from "util"
import { Metrics } from "./metrics"
import { Client, DeclareConsumerParams } from "rabbitmq-stream-js-client"
import { Logger } from "winston"
import { ConsumerFunc } from "../dist/consumer"

export class PerfTestConsumer {
  private readonly metrics: Metrics
  private displayTimer: NodeJS.Timeout | null

  constructor(
    private readonly client: Client,
    private readonly logger: Logger,
    private readonly consumerParams: DeclareConsumerParams,
    private readonly expected: number
  ) {
    this.metrics = new Metrics()
  }

  async cycle() {
    let ctr = 0
    /*    this.displayTimer = setInterval(() => {
      this.displayMetrics()
      this.metrics.setStart()
    }, 1000)*/
    const handle: ConsumerFunc = (_msg) => {
      this.metrics.addCounter("consumed")
      ctr++
    }
    this.metrics.setStart()
    await this.client.declareConsumer(this.consumerParams, handle)

    return new Promise((res) => {
      setInterval(() => {
        if (ctr >= this.expected) {
          this.displayMetrics()
          this.metrics.setStart()
          res(true)
        }
      }, 1000)
    })
  }

  private displayMetrics(stop: boolean = false) {
    const metrics = { ...this.metrics.getMetrics() }
    this.logger.info(`${inspect(metrics)}`)
    if (stop && this.displayTimer) {
      clearInterval(this.displayTimer)
    }
  }
}
