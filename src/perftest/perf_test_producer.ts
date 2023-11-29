import { inspect } from "util"
import { Connection, DeclarePublisherParams } from "../connection"
import { Metrics } from "./metrics"
import { writeFile } from "fs"

export class PerfTestProducer {
  private readonly metrics = new Metrics()
  private payload: Buffer

  constructor(
    private readonly connection: Connection,
    private readonly publisherParams: DeclarePublisherParams,
    byte_length: number = 10
  ) {
    this.payload = Buffer.alloc(byte_length, Math.random().toString())
    console.log("in perf test constructor. Payload " + inspect(this.payload))
    const dtnow = new Date()
    writeFile(`./${dtnow.toISOString()}_input_payload.bin`, this.payload, "binary", (_err) => {
      null
    })
  }

  public async cycle() {
    console.log("init cycle")
    const publisher = await this.connection.declarePublisher(this.publisherParams)
    publisher.on("publish_confirm", (_err, _confirmedIds) => {
      //this.metrics.addCounter("confirmed", confirmedIds.length)
    })

    this.metrics.setStart()

    let ctr = 0
    const max = 500000

    while (ctr < max) {
      await publisher.send(this.payload, {})

      this.metrics.addCounter("published")
      ctr = ctr + 1

      if (this.metrics.getCurrentDelta() > 1000) {
        this.displayMetrics()
        this.metrics.setStart()
      }
    }
  }

  private displayMetrics() {
    const metrics = this.metrics.getMetrics()
    console.log(inspect(metrics))
  }
}
