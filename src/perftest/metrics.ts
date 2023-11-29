import { inspect } from "node:util"

export class Metrics {
  private metrics: { [key: string]: number | undefined } = {}
  private ts_start: number = 0
  private ts_end: number = 0

  public addCounter(metricName: string, value: number = 1) {
    this.metrics[metricName] = (this.metrics[metricName] || 0) + value
  }

  public reset(metricName: string) {
    delete this.metrics[metricName]
  }

  public setStart() {
    this.ts_start = Date.now()
  }

  public getMetrics() {
    this.ts_end = Date.now()
    const delta = this.ts_end - this.ts_start
    console.log(inspect(this.metrics))
    const result = { ...this.metrics, delta: delta }

    this.resetAll()

    console.log(`after reset ${inspect(this.metrics)}`)

    return result
  }

  public getCurrentDelta() {
    const dt_now = Date.now()

    return dt_now - this.ts_start
  }

  private resetAll() {
    Object.keys(this.metrics).forEach((k) => this.reset(k))
    this.ts_start = 0
    this.ts_end = 0
  }
}
