export type CreditRequestWrapper = (howMany: number) => Promise<void>

export abstract class ConsumerCreditPolicy {
  constructor(protected readonly onStartup: number) {}

  public onChunkReceived(_requestWrapper: CreditRequestWrapper) {
    return
  }
  public onChunkProgress(_idx: number, _total: number, _requestWrapper: CreditRequestWrapper) {
    return
  }
  public onChunkCompleted(_requestWrapper: CreditRequestWrapper) {
    return
  }

  public onSubscription() {
    return this.onStartup
  }
}

class NewCreditsOnChunkReceived extends ConsumerCreditPolicy {
  constructor(onStartup: number = 1, private readonly onRenewal: number = 1) {
    super(onStartup)
  }

  public async onChunkReceived(requestWrapper: CreditRequestWrapper) {
    await requestWrapper(this.onRenewal)
  }

  public onSubscription(): number {
    return this.onStartup
  }
}

class NewCreditsOnChunkProgress extends ConsumerCreditPolicy {
  constructor(onStartup: number = 1, private readonly ratio: number = 0.5, private readonly howMany: number = 1) {
    super(onStartup)
  }

  public async onChunkProgress(current: number, total: number, requestWrapper: CreditRequestWrapper) {
    const threshold = Math.max(1, Math.ceil(this.ratio * total))

    if (current === threshold) {
      await requestWrapper(this.howMany)
    }
  }
}

class NewCreditsOnChunkCompleted extends ConsumerCreditPolicy {
  constructor(onStartup: number = 1, private readonly howMany: number = 1) {
    super(onStartup)
  }

  public async onChunkCompleted(requestWrapper: CreditRequestWrapper) {
    await requestWrapper(this.howMany)
  }
}

export const creditsOnChunkReceived = (onStartup: number, onRenewal: number) =>
  new NewCreditsOnChunkReceived(onStartup, onRenewal)
export const creditsOnChunkProgress = (onStartup: number, ratio: number, onRenewal: number) =>
  new NewCreditsOnChunkProgress(onStartup, ratio, onRenewal)
export const creditsOnChunkCompleted = (onStartup: number, onRenewal: number) =>
  new NewCreditsOnChunkCompleted(onStartup, onRenewal)
export const defaultCreditPolicy = creditsOnChunkReceived(2, 1)
