import { Connection } from "./connection"
import { getMaxSharedConnectionInstances } from "./util"

type InstanceKey = string

export class ConnectionPool {
  private static consumerConnectionProxies = new Map<InstanceKey, Connection[]>()
  private static publisherConnectionProxies = new Map<InstanceKey, Connection[]>()

  public static getUsableCachedConnection(leader: boolean, streamName: string, host: string, index: number) {
    const m = leader ? ConnectionPool.publisherConnectionProxies : ConnectionPool.consumerConnectionProxies
    const k = ConnectionPool.getCacheKey(streamName, host, index)
    const proxies = m.get(k) || []
    const connection = proxies.at(-1)
    const refCount = connection?.refCount
    return refCount !== undefined && refCount < getMaxSharedConnectionInstances() ? connection : undefined
  }

  public static cacheConnection(leader: boolean, streamName: string, host: string, client: Connection) {
    const m = leader ? ConnectionPool.publisherConnectionProxies : ConnectionPool.consumerConnectionProxies
    const k = ConnectionPool.getCacheKey(streamName, host, client.index)
    const currentlyCached = m.get(k) || []
    currentlyCached.push(client)
    m.set(k, currentlyCached)
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
    const k = ConnectionPool.getCacheKey(streamName, host, connection.index)
    const mappedClientList = m.get(k)
    if (mappedClientList) {
      const filtered = mappedClientList.filter((c) => c !== connection)
      m.set(k, filtered)
    }
  }

  private static getCacheKey(streamName: string, host: string, index: number) {
    return `${streamName}-${index}@${host}`
  }
}
