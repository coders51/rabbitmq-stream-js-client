export function removeFrom<T>(l: T[], predicate: (x: T) => boolean): T | undefined {
  const i = l.findIndex(predicate)
  if (i === -1) return
  const [e] = l.splice(i, 1)
  return e
}

export function range(count: number): number[] {
  const ret = Array(count)
  for (let index = 0; index < count; index++) {
    ret[index] = index
  }
  return ret
}

export const DEFAULT_FRAME_MAX = 1048576
export const DEFAULT_UNLIMITED_FRAME_MAX = 0
export const REQUIRED_MANAGEMENT_VERSION = "3.13.0"
export const getMaxSharedClientInstances = () => {
  return +(process.env.MAX_SHARED_CLIENT_INSTANCES ?? 100)
}

export const getAddressResolverFromEnv = (): { host: string; port: number } => {
  const envValue = process.env.RABBIT_MQ_TEST_ADDRESS_BALANCER ?? "localhost:5552"
  const [host, port] = envValue.split(":")
  return { host: host ?? "localhost", port: parseInt(port) ?? 5553 }
}

export const sample = <T>(items: (T | undefined)[]): T | undefined => {
  const actualItems = items.filter((c) => !!c)
  if (!actualItems.length) {
    return undefined
  }
  const index = Math.floor(Math.random() * actualItems.length)
  return actualItems[index]!
}
