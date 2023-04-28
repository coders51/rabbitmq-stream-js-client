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

export async function expectToThrowAsync(
  method: () => Promise<unknown>,
  expectedError: Function | Error,
  errorMessage: string | RegExp | null = null
): Promise<void> {
  let error = null
  try {
    await method()
  } catch (err) {
    error = err
  }
  expect(error).instanceOf(expectedError)
  if (errorMessage instanceof RegExp) {
    expect((error as { message: string }).message).match(errorMessage)
  }
  if (typeof errorMessage === "string") {
    expect((error as { message: string }).message).eql(errorMessage)
  }
}

export function wait(timeout: number) {
  return new Promise((res) => setTimeout(res, timeout))
}

export function range(count: number): number[] {
  const ret = Array(count)
  for (let index = 0; index < count; index++) {
    ret[index] = index
  }
  return ret
}
