import { AssertionError, expect } from "chai"

export function elapsedFrom(from: number): number {
  return Date.now() - from
}

export async function eventually(fn: Function, timeout = 1500) {
  const start = Date.now()
  while (true) {
    try {
      await fn()
      return
    } catch (error) {
      if (elapsedFrom(start) > timeout) {
        if (error instanceof AssertionError) throw error
        expect.fail(error as string)
      }
      await wait(5)
    }
  }
}

export function wait(timeout: number) {
  return new Promise((res) => setTimeout(res, timeout))
}
