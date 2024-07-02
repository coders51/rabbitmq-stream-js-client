export type CreditRequestWrapper = (howMany: number) => Promise<void>

export abstract class ConsumerCreditPolicy {
  constructor(protected readonly startFrom: number) {}

  public async onChunkReceived(_requestWrapper: CreditRequestWrapper) {
    return
  }

  public async onChunkCompleted(_requestWrapper: CreditRequestWrapper) {
    return
  }

  public async requestCredits(requestWrapper: CreditRequestWrapper, amount: number) {
    return requestWrapper(amount)
  }

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

export const creditsOnChunkReceived = (startFrom: number, step: number) =>
  new NewCreditsOnChunkReceived(startFrom, step)
export const creditsOnChunkCompleted = (startFrom: number, step: number) =>
  new NewCreditsOnChunkCompleted(startFrom, step)
export const defaultCreditPolicy = creditsOnChunkReceived(2, 1)
