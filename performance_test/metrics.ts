export class Metrics {
  private metrics: { [key: string]: number | undefined } = {
    published: 0,
    confirmed: 0,
  }
  private ts_start: number = 0
  private ts_end: number = 0

  public addCounter(metricName: string, value: number = 1) {
    this.metrics[metricName] = (this.metrics[metricName] || 0) + value
  }

  public reset(metricName: string) {
    this.metrics[metricName] = 0
  }

  public setStart() {
    this.ts_start = Date.now()
  }

  public getMetrics() {
    this.ts_end = Date.now()
    const delta = this.ts_end - this.ts_start
    const timedMetrics = Object.fromEntries(
      this.getMetricsNames().map((k) => [`${k}/s`, this.getTimedMetric(k, delta)])
    )
    const result = { ...this.metrics, ...timedMetrics, delta }

    this.resetAll()

    return result
  }

  public getCurrentDelta() {
    const dt_now = Date.now()

    return dt_now - this.ts_start
  }

  private resetAll() {
    this.getMetricsNames().forEach((k) => this.reset(k))
    this.ts_start = 0
    this.ts_end = 0
  }

  private getMetricsNames() {
    return Object.keys(this.metrics)
  }

  private getTimedMetric(metricName: string, delta: number) {
    if (delta < 1) return 0

    const v = this.metrics[metricName] || 0

    return +((v / delta) * 1000).toFixed(0)
  }
}
