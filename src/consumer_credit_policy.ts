export type CreditRequestWrapper = (howMany: number) => Promise<void>

export abstract class ConsumerCreditPolicy {
  constructor(protected readonly startFrom: number) {}

  public async onChunkReceived(_requestWrapper: CreditRequestWrapper) {
    return
  }
  public async onChunkProgress(_idx: number, _total: number, _requestWrapper: CreditRequestWrapper) {
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
  constructor(startFrom: number = 1, private readonly step: number = 1) {
    super(startFrom)
  }

  public async onChunkReceived(requestWrapper: CreditRequestWrapper) {
    await this.requestCredits(requestWrapper, this.step)
  }

  public onSubscription(): number {
    return this.startFrom
  }
}

class NewCreditsOnChunkProgress extends ConsumerCreditPolicy {
  constructor(onStartup: number = 1, private readonly ratio: number = 0.5, private readonly step: number = 1) {
    super(onStartup)
  }

  public async onChunkProgress(current: number, total: number, requestWrapper: CreditRequestWrapper) {
    const threshold = Math.max(1, Math.ceil(this.ratio * total))

    if (current === threshold) {
      await this.requestCredits(requestWrapper, this.step)
    }
  }
}

class NewCreditsOnChunkCompleted extends ConsumerCreditPolicy {
  constructor(onStartup: number = 1, private readonly step: number = 1) {
    super(onStartup)
  }

  public async onChunkCompleted(requestWrapper: CreditRequestWrapper) {
    await this.requestCredits(requestWrapper, this.step)
  }
}

export const creditsOnChunkReceived = (onStartup: number, step: number) =>
  new NewCreditsOnChunkReceived(onStartup, step)
export const creditsOnChunkProgress = (onStartup: number, ratio: number, step: number) =>
  new NewCreditsOnChunkProgress(onStartup, ratio, step)
export const creditsOnChunkCompleted = (onStartup: number, step: number) =>
  new NewCreditsOnChunkCompleted(onStartup, step)
export const defaultCreditPolicy = creditsOnChunkReceived(2, 1)
