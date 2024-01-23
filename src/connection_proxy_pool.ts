import { ConnectionProxy } from "./connection_proxy"
import { getMaxSharedConnectionInstances } from "./util"

type InstanceKey = string

export class ConnectionProxyPool {
  private static consumerConnectionProxies = new Map<InstanceKey, ConnectionProxy[]>()
  private static publisherConnectionProxies = new Map<InstanceKey, ConnectionProxy[]>()

  public static getUsableCachedConnectionProxy(leader: boolean, streamName: string, host: string) {
    const m = leader ? ConnectionProxyPool.publisherConnectionProxies : ConnectionProxyPool.consumerConnectionProxies
    const k = ConnectionProxyPool.getCacheKey(streamName, host)
    const proxies = m.get(k) || []
    const connectionProxy = proxies.at(-1)
    const refCount = connectionProxy?.refCount
    return refCount !== undefined && refCount < getMaxSharedConnectionInstances() ? connectionProxy : undefined
  }

  public static cacheConnectionProxy(leader: boolean, streamName: string, host: string, client: ConnectionProxy) {
    const m = leader ? ConnectionProxyPool.publisherConnectionProxies : ConnectionProxyPool.consumerConnectionProxies
    const k = ConnectionProxyPool.getCacheKey(streamName, host)
    const currentlyCached = m.get(k) || []
    currentlyCached.push(client)
    m.set(k, currentlyCached)
  }

  public static removeIfUnused(connectionProxy: ConnectionProxy) {
    if (connectionProxy.refCount <= 0) {
      ConnectionProxyPool.removeCachedConnectionProxy(connectionProxy)
      return true
    }
    return false
  }

  public static removeCachedConnectionProxy(connectionProxy: ConnectionProxy) {
    const leader = connectionProxy.leader
    const streamName = connectionProxy.streamName
    const host = connectionProxy.hostname
    if (streamName === undefined) return
    const m = leader ? ConnectionProxyPool.publisherConnectionProxies : ConnectionProxyPool.consumerConnectionProxies
    const k = ConnectionProxyPool.getCacheKey(streamName, host)
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
