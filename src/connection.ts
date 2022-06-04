import { Socket } from "net"
import { inspect } from "util"
import { createLogger, format, transports } from "winston"

interface Command {
  toBuffer(correlationId: number): Buffer
  // commandSize(): number
  readonly key: number
  readonly version: 1
  // getCorrelationId: () => number
  // getSizeNeed: () => string
  // append: (buffer: Buffer) => void
}

class PeerPropertiesResponse {
  key = 0x11
}

// const sizeLength = 4
// const keyLength = 2
// const versionLength = 2
// const correlationIdLength = 4

const PROPERTIES = {
  product: "RabbitMQ Stream",
  version: "0.0.1",
  platform: "javascript",
  copyright: "Copyright (c) 2020-2021 VMware, Inc. or its affiliates.",
  information: "Licensed under the Apache 2.0 and MPL 2.0 licenses. See https://www.rabbitmq.com/",
  connection_name: "Unknown",
}

class PeerPropertiesRequest implements Command {
  readonly key = 0x11
  readonly version = 1
  private readonly _properties: { key: string; value: string }[] = []

  constructor(properties: Record<string, string> = PROPERTIES) {
    this._properties = Object.keys(properties).map((key) => ({ key, value: properties[key] }))
  }

  // (2) Key => uint16 // 0x0011
  // (2) Version => uint16
  // (4) CorrelationId => uint32
  // (4 -> size + array)PeerProperties => [PeerProperty]
  // PeerProperty => Key Value
  // Key => string
  // Value => string

  toBuffer(correlationId: number): Buffer {
    let offset = 4
    const b = Buffer.alloc(1024)
    offset = b.writeUInt16BE(this.key, offset)
    offset = b.writeUInt16BE(this.version, offset)
    offset = b.writeUInt32BE(correlationId, offset)
    offset = b.writeUInt32BE(this._properties.length, offset)

    this._properties.forEach(({ key, value }) => {
      offset = this.writeString(b, offset, key)
      offset = this.writeString(b, offset, value)
    })

    b.writeUInt32BE(offset - 4, 0)
    return b.slice(0, offset)
  }

  writeString(buffer: Buffer, offset: number, s: string) {
    const newOffset = buffer.writeInt16BE(s.length, offset)
    const written = buffer.write(s, newOffset, "utf8")
    return newOffset + written
  }
}

export class Connection {
  private readonly socket = new Socket()
  private readonly logger = createLogger({
    silent: false,
    level: "debug",
    format: format.combine(
      format.colorize(),
      format.timestamp(),
      format.align(),
      format.splat(),
      format.label(),
      format.printf((info) => `${info.timestamp} ${info.level}: ${info.message} ${info.meta ? inspect(info.meta) : ""}`)
    ),
    transports: new transports.Console(),
  })
  private correlationId = 100

  static connect(params: ConnectionParams): Promise<Connection> {
    const c = new Connection()
    return c.start(params)
  }

  start(params: ConnectionParams): Promise<Connection> {
    return new Promise((res, rej) => {
      this.socket.on("error", (err) => {
        this.logger.warn(`Error on connection ${params.hostname}:${params.port} vhost:${params.vhost} err: ${err}`)
        return rej(err)
      })
      this.socket.on("connect", async () => {
        this.logger.info(`Connected to RabbitMQ ${params.hostname}:${params.port}`)
        await this.exchangeProperties()
        await this.auth()
        await this.tune()
        await this.open()
        return res(this)
      })
      this.socket.on("drain", () => this.logger.warn(`Draining ${params.hostname}:${params.port}`))
      this.socket.on("timeout", () => {
        this.logger.error(`Timeout ${params.hostname}:${params.port}`)
        return rej()
      })
      this.socket.on("data", (data) => {
        this.logger.debug(`receiving data ...`)
        this.logger.debug(inspect(data))
      })
      this.socket.on("close", (had_error) => {
        this.logger.info(`Close event on socket, close cloud had_error? ${had_error}`)
      })
      this.socket.connect(params.port, params.hostname)
    })

    console.log("establish_connection")
  }
  open() {
    throw new Error("Method not implemented.")
  }
  tune() {
    throw new Error("Method not implemented.")
  }
  auth() {
    throw new Error("Method not implemented.")
  }

  exchangeProperties(): Promise<void> {
    this.logger.debug(`Exchange properties ...`)
    const req = new PeerPropertiesRequest()
    return this.send<PeerPropertiesResponse>(req).then((res) => {
      this.logger.debug(inspect(res))
    })
  }

  send<T>(cmd: Command): Promise<T> {
    return new Promise((res, rej) => {
      const correlationId = this.incCorrelationId()
      const body = cmd.toBuffer(correlationId)
      this.logger.debug(
        `Write cmd key: ${cmd.key} - correlationId: ${correlationId}: data: ${inspect(body.toJSON())} length: ${
          body.byteLength
        }`
      )
      this.socket.write(body, (err) => {
        this.logger.debug(`Write COMPLETED for cmd key: ${cmd.key} - correlationId: ${correlationId} err: ${err}`)
        if (err) {
          return rej(err)
        }
        return res
      })
    })
  }

  incCorrelationId() {
    this.correlationId += 1
    return this.correlationId
  }

  close(): Promise<void> {
    this.logger.info(`Closing connection ...`)
    return new Promise((res, _rej) => this.socket.end(() => res()))
  }
}

export interface ConnectionParams {
  hostname: string
  port: number
  username: string
  password: string
  vhost: string
  frameMax: number // not used
  heartbeat: number // not user
}

export function connect(params: ConnectionParams): Promise<Connection> {
  return Connection.connect(params)
}

function wait(timeout: number) {
  return new Promise((res) => setTimeout(res, timeout))
}
