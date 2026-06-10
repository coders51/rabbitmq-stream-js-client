import { Client } from "./client"
import { Consumer } from "./consumer"
import { ConsumerCreditPolicy, defaultCreditPolicy } from "./consumer_credit_policy"
import { Message } from "./publisher"
import { Offset } from "./requests/subscribe_request"

/**
 * Handler invoked for every message received from a super stream
 *
 * @param msg - The received message
 * @param consumer - The partition consumer that delivered the message
 */
export type SuperStreamConsumerFunc = (msg: Message, consumer: Consumer) => Promise<void> | void

/**
 * Consumes messages from a super stream
 *
 * A super stream is a logical stream made of several partitions, each one a
 * regular stream. This consumer declares one single-active {@link Consumer} per
 * partition, all sharing the same consumer reference, so that messages from
 * every partition are delivered to a single handler.
 */
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

  /** Declare a single-active consumer on every partition of the super stream */
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

  /**
   * Create and start a super stream consumer
   *
   * Declares the per-partition consumers so the handler starts receiving messages.
   *
   * @param handle - The handler invoked for every received message
   */
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

  /** Close every per-partition consumer */
  async close(): Promise<void> {
    await Promise.all([...this.consumers.values()].map((c) => c.close()))
  }
}
