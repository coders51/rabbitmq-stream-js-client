import { Connection } from "./connection"
import { getMaxSharedConnectionInstances } from "./util"

type InstanceKey = string
export type ConnectionPurpose = "consumer" | "publisher"

export class ConnectionPool {
  private consumerConnectionProxies: Map<InstanceKey, Connection[]> = new Map<InstanceKey, Connection[]>()
  private publisherConnectionProxies: Map<InstanceKey, Connection[]> = new Map<InstanceKey, Connection[]>()

  public getCachedConnection(purpose: ConnectionPurpose, streamName: string, vhost: string, host: string) {
    const map = purpose === "publisher" ? this.publisherConnectionProxies : this.consumerConnectionProxies
    const key = this.getCacheKey(streamName, vhost, host)
    const proxies = map.get(key) || []
    const connection = proxies.at(-1)
    const refCount = connection?.refCount
    return refCount !== undefined && refCount < getMaxSharedConnectionInstances() ? connection : undefined
  }

  public cacheConnection(
    purpose: ConnectionPurpose,
    streamName: string,
    vhost: string,
    host: string,
    client: Connection
  ) {
    const map = purpose === "publisher" ? this.publisherConnectionProxies : this.consumerConnectionProxies
    const key = this.getCacheKey(streamName, vhost, host)
    const currentlyCached = map.get(key) || []
    currentlyCached.push(client)
    map.set(key, currentlyCached)
  }

  public removeIfUnused(connection: Connection) {
    if (connection.refCount <= 0) {
      this.removeCachedConnection(connection)
      return true
    }
    return false
  }

  public removeCachedConnection(connection: Connection) {
    const { leader, streamName, hostname: host, vhost } = connection
    if (streamName === undefined) return
    const m = leader ? this.publisherConnectionProxies : this.consumerConnectionProxies
    const k = this.getCacheKey(streamName, vhost, host)
    const mappedClientList = m.get(k)
    if (mappedClientList) {
      const filtered = mappedClientList.filter((c) => c !== connection)
      m.set(k, filtered)
    }
  }

  private getCacheKey(streamName: string, vhost: string, host: string) {
    return `${streamName}@${vhost}@${host}`
  }
}
