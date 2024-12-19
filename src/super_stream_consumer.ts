import { Client } from "./client"
import { Consumer, ConsumerFunc } from "./consumer"
import { Offset } from "./requests/subscribe_request"

export class SuperStreamConsumer {
  private consumers: Map<string, Consumer> = new Map<string, Consumer>()
  public consumerRef: string
  readonly superStream: string
  private locator: Client
  private partitions: string[]
  private offset: Offset

  private constructor(
    readonly handle: ConsumerFunc,
    params: {
      superStream: string
      locator: Client
      partitions: string[]
      consumerRef: string
      offset: Offset
    }
  ) {
    this.superStream = params.superStream
    this.consumerRef = params.consumerRef
    this.locator = params.locator
    this.partitions = params.partitions
    this.offset = params.offset
  }

  async start(): Promise<void> {
    await Promise.all(
      this.partitions.map(async (p) => {
        const partitionConsumer = await this.locator.declareConsumer(
          { stream: p, consumerRef: this.consumerRef, offset: this.offset, singleActive: true },
          this.handle,
          this
        )
        this.consumers.set(p, partitionConsumer)
        return
      })
    )
  }

  static async create(
    handle: ConsumerFunc,
    params: {
      superStream: string
      locator: Client
      partitions: string[]
      consumerRef: string
      offset: Offset
    }
  ): Promise<SuperStreamConsumer> {
    const superStreamConsumer = new SuperStreamConsumer(handle, params)
    await superStreamConsumer.start()
    return superStreamConsumer
  }

  async close(): Promise<void> {
    await Promise.all([...this.consumers.values()].map((c) => c.close(true)))
  }
}
