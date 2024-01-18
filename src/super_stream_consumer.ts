import { Client } from "./client"
import { Consumer, ConsumerFunc } from "./consumer"
import { Offset } from "./requests/subscribe_request"

export class SuperStreamConsumer {
  private consumers: Map<string, Consumer> = new Map<string, Consumer>()
  public consumerRef: string
  private locator: Client
  private partitions: string[]

  private constructor(
    readonly handle: ConsumerFunc,
    params: {
      locator: Client
      partitions: string[]
      consumerRef: string
    },
  ) {
    this.consumerRef = params.consumerRef
    this.locator = params.locator
    this.partitions = params.partitions
  }

  async start(): Promise<void> {
    await Promise.all(
      this.partitions.map(async (p) => {
        const partitionConsumer = await this.locator.declareConsumer(
          { stream: p, consumerRef: this.consumerRef, offset: Offset.first(), singleActive: true },
          this.handle,
        )
        this.consumers.set(p, partitionConsumer)
        return
      }),
    )
  }

  static async create(
    handle: ConsumerFunc,
    params: {
      locator: Client
      partitions: string[]
      consumerRef: string
    },
  ): Promise<SuperStreamConsumer> {
    const superStreamConsumer = new SuperStreamConsumer(handle, params)
    await superStreamConsumer.start()
    return superStreamConsumer
  }

  async close(): Promise<void> {
    await Promise.all([...this.consumers.values()].map((c) => c.close()))
  }
}
