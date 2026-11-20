/**
 * Thrown when a command cannot be sent because the connection's socket is not open,
 * for example after the connection was lost or closed.
 */
export class SocketNotOpenError extends Error {
  constructor(public readonly connectionId: string) {
    super(`Socket is not open for connection ${connectionId}`)
    this.name = this.constructor.name
  }
}
