import { inspect } from "util"
import { Connection } from "./connection"
import { Logger } from "./logger"
import { getMaxSharedConnectionInstances } from "./util"

type InstanceKey = string
export type ConnectionPurpose = "consumer" | "publisher"

export class ConnectionPool {
  private connectionsMap: Map<InstanceKey, Connection[]> = new Map<InstanceKey, Connection[]>()

  constructor(private readonly log: Logger) {}

  public async getConnection(
    entityType: ConnectionPurpose,
    streamName: string,
    vhost: string,
    host: string,
    connectionCreator: () => Promise<Connection>
  ) {
    const key = this.getCacheKey(streamName, vhost, host, entityType)
    const connections = this.connectionsMap.get(key) || []
    const connection = connections.at(-1)
    const refCount = connection?.refCount
    const cachedConnection =
      refCount !== undefined && refCount < getMaxSharedConnectionInstances() ? connection : undefined

    if (cachedConnection) {
      return cachedConnection
    } else {
      const newConnection = await connectionCreator()
      this.cacheConnection(key, newConnection)
      return newConnection
    }
  }

  public async releaseConnection(connection: Connection, manuallyClose = true): Promise<void> {
    connection.decrRefCount()
    if (connection.refCount <= 0 && connection.ready) {
      try {
        await connection.close({ closingCode: 0, closingReason: "", manuallyClose })
      } catch (e) {
        // in case the client is closed immediately after a consumer, its connection has still not
        // reset the ready flag, so we get an "Error: write after end"
        this.log.warn(`Could not close connection: ${inspect(e)}`)
      }
      this.removeCachedConnection(connection)
    }
  }

  private cacheConnection(key: string, connection: Connection) {
    const currentlyCached = this.connectionsMap.get(key) || []
    currentlyCached.push(connection)
    this.connectionsMap.set(key, currentlyCached)
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
