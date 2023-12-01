import { Connection, DeclarePublisherParams } from "../connection"
import { Metrics } from "./metrics"
import { Producer } from "../producer"
import { inspect } from "util"

export class PerfTestProducer {
  private readonly metrics = new Metrics()
  private payload: Buffer
  private readonly maxMessages: number = -1
  private readonly maxChunkSize: number = 1000
  private ctr = 0

  constructor(
    private readonly connection: Connection,
    private readonly publisherParams: DeclarePublisherParams,
    byteLength: number = 10
  ) {
    this.payload = Buffer.alloc(byteLength, Math.random().toString())
    console.log(
      `PerfTest - Payload Size: ${this.payload.length} bytes - Max buffer length: ${publisherParams.maxBufferLength} - Send chunk size: ${publisherParams.chunkSize}  - Max send duration: ${publisherParams.sendDuration} ms`
    )
    this.metrics.setStart()
  }

  public async cycle() {
    const publisher = await this.connection.declarePublisher(this.publisherParams)
    publisher.on("publish_confirm", (err, confirmedIds) => {
      if (err) {
        console.log(err)
      }
      this.metrics.addCounter("confirmed", confirmedIds.length)
    })

    setInterval(() => {
      this.displayMetrics()
      this.metrics.setStart()
    }, 1000)

    await this.send(publisher)
  }

  private displayMetrics() {
    const metrics = { ...this.metrics.getMetrics(), total: this.ctr }
    console.log(`${new Date().toISOString()} - ${inspect(metrics)}`)
  }

  private async send(publisher: Producer) {
    while (this.maxMessages === -1 || this.ctr < this.maxMessages) {
      const nmsgs = this.maxMessages > 0 ? Math.min(this.maxChunkSize, this.maxMessages) : this.maxChunkSize
      for (let index = 0; index < nmsgs; index++) {
        await publisher.send(this.payload, {})
      }
      this.ctr = this.ctr + nmsgs
      this.metrics.addCounter("published", nmsgs)
    }
  }
}
