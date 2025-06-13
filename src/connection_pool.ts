import { Connection } from "./connection"
import { getMaxSharedConnectionInstances } from "./util"

type InstanceKey = string
export type ConnectionPurpose = "consumer" | "publisher"

export class ConnectionPool {
  private consumerConnectionProxies: Map<InstanceKey, Connection[]> = new Map<InstanceKey, Connection[]>()
  private publisherConnectionProxies: Map<InstanceKey, Connection[]> = new Map<InstanceKey, Connection[]>()

  public async getConnection(
    purpose: ConnectionPurpose,
    streamName: string,
    vhost: string,
    host: string,
    connectionCreator: () => Promise<Connection>
  ) {
    const map = purpose === "publisher" ? this.publisherConnectionProxies : this.consumerConnectionProxies
    const key = this.getCacheKey(streamName, vhost, host)
    const proxies = map.get(key) || []
    const connection = proxies.at(-1)
    const refCount = connection?.refCount
    const cachedConnection =
      refCount !== undefined && refCount < getMaxSharedConnectionInstances() ? connection : undefined

    if (cachedConnection) {
      return cachedConnection
    } else {
      const newConnection = await connectionCreator()
      this.cacheConnection(map, key, newConnection)
      return newConnection
    }
  }

  public async releaseConnection(connection: Connection, manuallyClose = true): Promise<void> {
    connection.decrRefCount()
    if (connection.refCount <= 0) {
      await connection.close({ closingCode: 0, closingReason: "", manuallyClose })
      this.removeCachedConnection(connection)
    }
  }

  private cacheConnection(map: Map<InstanceKey, Connection[]>, key: string, connection: Connection) {
    const currentlyCached = map.get(key) || []
    currentlyCached.push(connection)
    map.set(key, currentlyCached)
  }

  private removeCachedConnection(connection: Connection) {
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
