import { Connection } from "./connection"
import { getMaxSharedConnectionInstances } from "./util"

type InstanceKey = string
export type ConnectionPurpose = "consumer" | "publisher"

export class ConnectionPool {
  private static consumerConnectionProxies = new Map<InstanceKey, Connection[]>()
  private static publisherConnectionProxies = new Map<InstanceKey, Connection[]>()

  public static getUsableCachedConnection(purpose: ConnectionPurpose, streamName: string, host: string) {
    const map =
      purpose === "publisher" ? ConnectionPool.publisherConnectionProxies : ConnectionPool.consumerConnectionProxies
    const key = ConnectionPool.getCacheKey(streamName, host)
    const proxies = map.get(key) || []
    const connection = proxies.at(-1)
    const refCount = connection?.refCount
    return refCount !== undefined && refCount < getMaxSharedConnectionInstances() ? connection : undefined
  }

  public static cacheConnection(purpose: ConnectionPurpose, streamName: string, host: string, client: Connection) {
    const map =
      purpose === "publisher" ? ConnectionPool.publisherConnectionProxies : ConnectionPool.consumerConnectionProxies
    const key = ConnectionPool.getCacheKey(streamName, host)
    const currentlyCached = map.get(key) || []
    currentlyCached.push(client)
    map.set(key, currentlyCached)
  }

  public static removeIfUnused(connection: Connection) {
    if (connection.refCount <= 0) {
      ConnectionPool.removeCachedConnection(connection)
      return true
    }
    return false
  }

  public static removeCachedConnection(connection: Connection) {
    const { leader, streamName, hostname: host } = connection
    if (streamName === undefined) return
    const m = leader ? ConnectionPool.publisherConnectionProxies : ConnectionPool.consumerConnectionProxies
    const k = ConnectionPool.getCacheKey(streamName, host)
    const mappedClientList = m.get(k)
    if (mappedClientList) {
      const filtered = mappedClientList.filter((c) => c !== connection)
      m.set(k, filtered)
    }
  }

  private static getCacheKey(streamName: string, host: string) {
    return `${streamName}@${host}`
  }
}
