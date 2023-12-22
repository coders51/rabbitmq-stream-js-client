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

export const getTestNodesFromEnv = (): { host: string; port: number }[] => {
  const envValue = process.env.RABBIT_MQ_TEST_NODES ?? "localhost:5552"
  const nodes = envValue.split(";")
  return nodes.map((n) => {
    const [host, port] = n.split(":")
    return { host: host ?? "localhost", port: parseInt(port) ?? 5552 }
  })
}
