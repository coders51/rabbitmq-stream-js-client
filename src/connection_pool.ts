import { Connection } from "./connection"
import { getMaxSharedConnectionInstances } from "./util"

type InstanceKey = string

export class ConnectionPool {
  private static consumerConnectionProxies = new Map<InstanceKey, Connection[]>()
  private static publisherConnectionProxies = new Map<InstanceKey, Connection[]>()

  public static getUsableCachedConnectionProxy(leader: boolean, streamName: string, host: string) {
    const m = leader ? ConnectionPool.publisherConnectionProxies : ConnectionPool.consumerConnectionProxies
    const k = ConnectionPool.getCacheKey(streamName, host)
    const proxies = m.get(k) || []
    const connectionProxy = proxies.at(-1)
    const refCount = connectionProxy?.refCount
    return refCount !== undefined && refCount < getMaxSharedConnectionInstances() ? connectionProxy : undefined
  }

  public static cacheConnectionProxy(leader: boolean, streamName: string, host: string, client: Connection) {
    const m = leader ? ConnectionPool.publisherConnectionProxies : ConnectionPool.consumerConnectionProxies
    const k = ConnectionPool.getCacheKey(streamName, host)
    const currentlyCached = m.get(k) || []
    currentlyCached.push(client)
    m.set(k, currentlyCached)
  }

  public static removeIfUnused(connectionProxy: Connection) {
    if (connectionProxy.refCount <= 0) {
      ConnectionPool.removeCachedConnectionProxy(connectionProxy)
      return true
    }
    return false
  }

  public static removeCachedConnectionProxy(connectionProxy: Connection) {
    const leader = connectionProxy.leader
    const streamName = connectionProxy.streamName
    const host = connectionProxy.hostname
    if (streamName === undefined) return
    const m = leader ? ConnectionPool.publisherConnectionProxies : ConnectionPool.consumerConnectionProxies
    const k = ConnectionPool.getCacheKey(streamName, host)
    const mappedClientList = m.get(k)
    if (mappedClientList) {
      const filtered = mappedClientList.filter((cp) => cp !== connectionProxy)
      m.set(k, filtered)
    }
  }

  private static getCacheKey(streamName: string, host: string) {
    return `${streamName}@${host}`
  }
}
