import { Client, ConsumerFilter } from "./client"
import { Consumer } from "./consumer"
import { ConsumerCreditPolicy, defaultCreditPolicy } from "./consumer_credit_policy"
import { Message } from "./publisher"
import { Offset } from "./requests/subscribe_request"
import { isStreamResponseError } from "./stream_response_error"
import { ResponseCode } from "./util"

export type SuperStreamConsumerFunc = (msg: Message, consumer: Consumer) => Promise<void> | void

export class SuperStreamConsumer {
  private consumers: Map<string, Consumer> = new Map<string, Consumer>()
  public consumerRef: string
  readonly superStream: string
  private locator: Client
  private partitions: string[]
  private offset: Offset
  private creditPolicy: ConsumerCreditPolicy
  private filter?: ConsumerFilter

  private constructor(
    readonly handle: SuperStreamConsumerFunc,
    params: {
      superStream: string
      locator: Client
      partitions: string[]
      consumerRef: string
      offset: Offset
      creditPolicy?: ConsumerCreditPolicy
      filter?: ConsumerFilter
    }
  ) {
    this.superStream = params.superStream
    this.consumerRef = params.consumerRef
    this.locator = params.locator
    this.partitions = params.partitions
    this.offset = params.offset
    this.creditPolicy = params.creditPolicy || defaultCreditPolicy
    this.filter = params.filter
  }

  /**
   * Single active consumers are told where to start by the consumer update exchange. Resume after the
   * offset last stored on the server for this consumerRef and partition; if nothing has been stored yet
   * (e.g. a new consumerRef) use the offset the consumer was declared with.
   */
  private async resolveStartOffset(consumerRef: string, stream: string): Promise<Offset> {
    try {
      const stored = await this.locator.queryOffset({ reference: consumerRef, stream })
      return Offset.offset(stored + 1n)
    } catch (error) {
      if (isStreamResponseError(error) && error.code === ResponseCode.NoOffset) {
        return this.offset
      }
      throw error
    }
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
            consumerUpdateListener: (ref, stream) => this.resolveStartOffset(ref, stream),
            creditPolicy: this.creditPolicy,
            filter: this.filter,
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
      filter?: ConsumerFilter
    }
  ): Promise<SuperStreamConsumer> {
    const superStreamConsumer = new SuperStreamConsumer(handle, params)
    await superStreamConsumer.start()
    return superStreamConsumer
  }

  async close(): Promise<void> {
    // closeConsumer throws for consumers the client no longer tracks (e.g. already closed by the client itself)
    const registered = new Set(this.locator.getConsumers().map(({ consumer }) => consumer.extendedId))
    await Promise.all(
      [...this.consumers.values()]
        .filter((c) => registered.has(c.extendedId))
        .map((c) => this.locator.closeConsumer(c.extendedId))
    )
    this.consumers.clear()
  }
}
