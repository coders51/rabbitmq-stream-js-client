import { Connection } from "./connection"
import { getMaxSharedConnectionInstances } from "./util"

type InstanceKey = string
export type ConnectionPurpose = "consumer" | "publisher"

export class ConnectionPool {
  private connectionsMap: Map<InstanceKey, Connection[]> = new Map<InstanceKey, Connection[]>()

  public async getConnection(
    entityType: ConnectionPurpose,
    streamName: string,
    vhost: string,
    host: string,
    connectionCreator: () => Promise<Connection>
  ) {
    const key = this.getCacheKey(streamName, vhost, host, entityType)
    const proxies = this.connectionsMap.get(key) || []
    const connection = proxies.at(-1)
    const refCount = connection?.refCount
    const cachedConnection =
      refCount !== undefined && refCount < getMaxSharedConnectionInstances() ? connection : undefined

    if (cachedConnection) {
      return cachedConnection
    } else {
      const newConnection = await connectionCreator()
      this.cacheConnection(this.connectionsMap, key, newConnection)
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
    const entityType = leader ? "publisher" : "consumer"
    const k = this.getCacheKey(streamName, vhost, host, entityType)
    const mappedClientList = this.connectionsMap.get(k)
    if (mappedClientList) {
      const filtered = mappedClientList.filter((c) => c !== connection)
      this.connectionsMap.set(k, filtered)
    }
  }

  private getCacheKey(streamName: string, vhost: string, host: string, entityType: ConnectionPurpose) {
    return `${streamName}@${vhost}@${host}@${entityType}`
  }
}
