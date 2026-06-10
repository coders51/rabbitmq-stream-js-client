/**
 * Callback used by a credit policy to grant a given number of credits to the broker
 *
 * @param howMany - The number of credits to request
 */
export type CreditRequestWrapper = (howMany: number) => Promise<void>

/**
 * Controls how a consumer grants flow-control credits to the broker
 *
 * In the stream protocol the broker only delivers chunks for which the
 * consumer has available credit. A credit policy decides how many credits
 * are granted on subscription and when new credits are requested (on chunk
 * received vs. on chunk completed), which in turn affects throughput and
 * ordering guarantees.
 *
 * Use the {@link creditsOnChunkReceived} and {@link creditsOnChunkCompleted}
 * factory functions to obtain an instance.
 */
export abstract class ConsumerCreditPolicy {
  constructor(protected readonly startFrom: number) {}

  /** Called when a chunk is received, before it has been processed */
  public async onChunkReceived(_requestWrapper: CreditRequestWrapper) {
    return
  }

  /** Called once a received chunk has been fully processed */
  public async onChunkCompleted(_requestWrapper: CreditRequestWrapper) {
    return
  }

  /** Request the given amount of credits from the broker */
  public async requestCredits(requestWrapper: CreditRequestWrapper, amount: number) {
    return requestWrapper(amount)
  }

  /** The number of credits to grant when the consumer subscribes */
  public onSubscription() {
    return this.startFrom
  }
}

class NewCreditsOnChunkReceived extends ConsumerCreditPolicy {
  constructor(
    startFrom: number = 1,
    private readonly step: number = 1
  ) {
    super(startFrom)
  }

  public async onChunkReceived(requestWrapper: CreditRequestWrapper) {
    await this.requestCredits(requestWrapper, this.step)
  }

  public onSubscription(): number {
    return this.startFrom
  }
}

class NewCreditsOnChunkCompleted extends ConsumerCreditPolicy {
  constructor(
    startFrom: number = 1,
    private readonly step: number = 1
  ) {
    super(startFrom)
  }

  public async onChunkCompleted(requestWrapper: CreditRequestWrapper) {
    await this.requestCredits(requestWrapper, this.step)
  }
}

/**
 * Create a credit policy that requests new credits as soon as a chunk is received,
 * before it has been processed
 *
 * This favours throughput but can lead to out-of-order processing, since the next
 * chunk may arrive while the current one is still being handled.
 *
 * @param startFrom - Credits granted when the consumer subscribes
 * @param step - Credits requested each time a chunk is received
 */
export const creditsOnChunkReceived = (startFrom: number, step: number) =>
  new NewCreditsOnChunkReceived(startFrom, step)

/**
 * Create a credit policy that requests new credits only once a chunk has been
 * fully processed
 *
 * This preserves in-order processing, as the next chunk is requested only after
 * the current one is done. This is the default policy.
 *
 * @param startFrom - Credits granted when the consumer subscribes
 * @param step - Credits requested each time a chunk is completed
 */
export const creditsOnChunkCompleted = (startFrom: number, step: number) =>
  new NewCreditsOnChunkCompleted(startFrom, step)

/** The default credit policy used by consumers: `creditsOnChunkCompleted(1, 1)` */
export const defaultCreditPolicy = creditsOnChunkCompleted(1, 1)
