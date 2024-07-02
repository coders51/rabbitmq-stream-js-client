import { PromiseResolver } from "./promise_resolver"
import { Response } from "./responses/response"

export class WaitingResponse<T extends Response> {
  constructor(
    private correlationId: number,
    private key: number,
    private promise: PromiseResolver<T>
  ) {}

  waitingFor(response: Response): boolean {
    const correlationFound = this.correlationId === response.correlationId
    if (correlationFound && this.key !== response.key) {
      throw new Error(
        `Waiting response correlationId: ${this.correlationId} but key mismatch waiting: ${this.key} found ${response.key}`
      )
    }
    return correlationFound
  }

  resolve(response: T) {
    this.promise.resolve(response)
  }
}
