import { Client } from "./client"
import { Consumer } from "./consumer"
import { ConsumerCreditPolicy, defaultCreditPolicy } from "./consumer_credit_policy"
import { Message } from "./publisher"
import { Offset } from "./requests/subscribe_request"

export type SuperStreamConsumerFunc = (msg: Message, consumer: Consumer) => Promise<void> | void

export class SuperStreamConsumer {
  private consumers: Map<string, Consumer> = new Map<string, Consumer>()
  public consumerRef: string
  readonly superStream: string
  private locator: Client
  private partitions: string[]
  private offset: Offset
  private creditPolicy: ConsumerCreditPolicy

  private constructor(
    readonly handle: SuperStreamConsumerFunc,
    params: {
      superStream: string
      locator: Client
      partitions: string[]
      consumerRef: string
      offset: Offset
      creditPolicy?: ConsumerCreditPolicy
    }
  ) {
    this.superStream = params.superStream
    this.consumerRef = params.consumerRef
    this.locator = params.locator
    this.partitions = params.partitions
    this.offset = params.offset
    this.creditPolicy = params.creditPolicy || defaultCreditPolicy
  }

  async start(): Promise<void> {
    await Promise.all(
      this.partitions.map(async (p) => {
        const partitionConsumer = await this.locator.declareConsumer(
          {
            stream: p,
            consumerRef: this.consumerRef,
            offset: this.offset,
            singleActive: true,
            creditPolicy: this.creditPolicy,
          },
          (msg) => {
            const consumer = this.consumers.get(p)
            if (consumer) {
              return this.handle(msg, consumer)
            }
          },
          this
        )
        this.consumers.set(p, partitionConsumer)
        return
      })
    )
  }

  static async create(
    handle: SuperStreamConsumerFunc,
    params: {
      superStream: string
      locator: Client
      partitions: string[]
      consumerRef: string
      offset: Offset
      creditPolicy?: ConsumerCreditPolicy
    }
  ): Promise<SuperStreamConsumer> {
    const superStreamConsumer = new SuperStreamConsumer(handle, params)
    await superStreamConsumer.start()
    return superStreamConsumer
  }

  async close(): Promise<void> {
    await Promise.all([...this.consumers.values()].map((c) => c.close()))
  }
}
