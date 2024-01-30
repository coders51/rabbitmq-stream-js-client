import { randomUUID } from "crypto"
import { Socket } from "net"
import tls from "node:tls"
import { inspect } from "util"
import { Compression, CompressionType, GzipCompression, NoneCompression } from "./compression"
import { Heartbeat } from "./heartbeat"
import { Logger } from "./logger"
import { CloseRequest } from "./requests/close_request"
import { ExchangeCommandVersionsRequest } from "./requests/exchange_command_versions_request"
import { OpenRequest } from "./requests/open_request"
import { PROPERTIES as PEER_PROPERTIES, PeerPropertiesRequest } from "./requests/peer_properties_request"
import { BufferSizeParams, BufferSizeSettings, Request } from "./requests/request"
import { SaslAuthenticateRequest } from "./requests/sasl_authenticate_request"
import { SaslHandshakeRequest } from "./requests/sasl_handshake_request"
import { TuneRequest } from "./requests/tune_request"
import {
  ConsumerUpdateQueryListener,
  DeliverListener,
  DeliverV2Listener,
  MetadataUpdateListener,
  PublishConfirmListener,
  PublishErrorListener,
  ResponseDecoder,
} from "./response_decoder"
import { CloseResponse } from "./responses/close_response"
import { ExchangeCommandVersionsResponse } from "./responses/exchange_command_versions_response"
import { OpenResponse } from "./responses/open_response"
import { PeerPropertiesResponse } from "./responses/peer_properties_response"
import { Response } from "./responses/response"
import { SaslAuthenticateResponse } from "./responses/sasl_authenticate_response"
import { SaslHandshakeResponse } from "./responses/sasl_handshake_response"
import { TuneResponse } from "./responses/tune_response"
import { DEFAULT_FRAME_MAX, DEFAULT_UNLIMITED_FRAME_MAX, REQUIRED_MANAGEMENT_VERSION, removeFrom } from "./util"
import { Version, checkServerDeclaredVersions, getClientSupportedVersions } from "./versions"
import { WaitingResponse } from "./waiting_response"
import { ClientListenersParams, ClientParams, ClosingParams, QueryOffsetParams, StoreOffsetParams } from "./client"
import { QueryPublisherResponse } from "./responses/query_publisher_response"
import { QueryPublisherRequest } from "./requests/query_publisher_request"
import { StoreOffsetRequest } from "./requests/store_offset_request"
import { QueryOffsetResponse } from "./responses/query_offset_response"
import { QueryOffsetRequest } from "./requests/query_offset_request"
import { coerce, lt } from "semver"

export type ConnectionClosedListener = (hadError: boolean) => void

export type ConnectionListenersParams = ClientListenersParams & {
  deliverV1?: DeliverListener
  deliverV2?: DeliverV2Listener
  consumer_update_query?: ConsumerUpdateQueryListener
}

export type ConnectionParams = ClientParams & {
  listeners?: ConnectionListenersParams
}

export type ConnectionInfo = {
  host: string
  port: number
  id: string
  readable?: boolean
  writable?: boolean
  localPort?: number
}

function extractHeartbeatInterval(heartbeatInterval: number, tuneResponse: TuneResponse): number {
  return heartbeatInterval === 0 ? tuneResponse.heartbeat : Math.min(heartbeatInterval, tuneResponse.heartbeat)
}

export class Connection {
  public readonly hostname: string
  public readonly leader: boolean
  public readonly streamName: string | undefined
  private socket: Socket
  private correlationId = 100
  private decoder: ResponseDecoder
  private receivedResponses: Response[] = []
  private waitingResponses: WaitingResponse<never>[] = []
  private heartbeat: Heartbeat
  private compressions = new Map<CompressionType, Compression>()
  private peerProperties: Record<string, string> = {}
  private readonly bufferSizeSettings: BufferSizeSettings
  private frameMax: number = DEFAULT_FRAME_MAX
  private connectionId: string
  private connectionClosedListener: ConnectionClosedListener | undefined
  private serverEndpoint: { host: string; port: number } = { host: "", port: 5552 }
  private readonly serverDeclaredVersions: Version[] = []
  private refs: number = 0
  private filteringEnabled: boolean = false

  constructor(private readonly params: ConnectionParams, private readonly logger: Logger) {
    this.hostname = params.hostname
    this.leader = params.leader ?? false
    this.streamName = params.streamName
    if (params.frameMax) this.frameMax = params.frameMax
    if (params.ssl) {
      this.socket = tls.connect(params.port, params.hostname, { ...params.ssl, rejectUnauthorized: false })
    } else {
      this.socket = new Socket()
      this.socket.connect(this.params.port, this.params.hostname)
    }
    if (params.socketTimeout) this.socket.setTimeout(params.socketTimeout)
    this.heartbeat = new Heartbeat(this, this.logger)
    this.compressions.set(CompressionType.None, NoneCompression.create())
    this.compressions.set(CompressionType.Gzip, GzipCompression.create())
    this.decoder = new ResponseDecoder((...args) => this.responseReceived(...args), this.logger)
    this.bufferSizeSettings = params.bufferSizeSettings || {}
    this.connectionId = randomUUID()
    this.connectionClosedListener = params.listeners?.connection_closed
  }

  public static connect(params: ConnectionParams, logger: Logger): Promise<Connection> {
    return new Connection(params, logger).start()
  }

  public static create(params: ConnectionParams, logger: Logger): Connection {
    return new Connection(params, logger)
  }

  public start(): Promise<Connection> {
    this.registerListeners(this.params.listeners)

    return new Promise((res, rej) => {
      this.socket.on("error", (err) => {
        this.logger.warn(
          `Error on client ${this.params.hostname}:${this.params.port} vhost:${this.params.vhost} err: ${err}`
        )
        return rej(err)
      })
      this.socket.on("connect", async () => {
        this.logger.info(`Connected to RabbitMQ ${this.params.hostname}:${this.params.port}`)
        this.peerProperties = (await this.exchangeProperties()).properties
        this.filteringEnabled = lt(coerce(this.rabbitManagementVersion)!, REQUIRED_MANAGEMENT_VERSION) ? false : true
        await this.auth({ username: this.params.username, password: this.params.password })
        const { heartbeat } = await this.tune(this.params.heartbeat ?? 0)
        await this.open({ virtualHost: this.params.vhost })
        this.heartbeat.start(heartbeat)
        await this.exchangeCommandVersions()
        return res(this)
      })
      this.socket.on("drain", () => this.logger.warn(`Draining ${this.params.hostname}:${this.params.port}`))
      this.socket.on("timeout", () => {
        this.logger.error(`Timeout ${this.params.hostname}:${this.params.port}`)
        return rej(new Error(`Timeout ${this.params.hostname}:${this.params.port}`))
      })
      this.socket.on("data", (data) => {
        this.heartbeat.reportLastMessageReceived()
        this.received(data)
      })
      this.socket.on("close", (had_error) => {
        this.logger.info(`Close event on socket, close cloud had_error? ${had_error}`)
        if (this.connectionClosedListener) this.connectionClosedListener(had_error)
      })
    })
  }

  public on(event: "metadata_update", listener: MetadataUpdateListener): void
  public on(event: "publish_confirm", listener: PublishConfirmListener): void
  public on(event: "publish_error", listener: PublishErrorListener): void
  public on(event: "deliverV1", listener: DeliverListener): void
  public on(event: "deliverV2", listener: DeliverV2Listener): void
  public on(event: "consumer_update_query", listener: ConsumerUpdateQueryListener): void
  public on(
    event:
      | "metadata_update"
      | "publish_confirm"
      | "publish_error"
      | "deliverV1"
      | "deliverV2"
      | "consumer_update_query",
    listener:
      | MetadataUpdateListener
      | PublishConfirmListener
      | PublishErrorListener
      | DeliverListener
      | DeliverV2Listener
      | ConsumerUpdateQueryListener
  ) {
    switch (event) {
      case "metadata_update":
        this.decoder.on("metadata_update", listener as MetadataUpdateListener)
        break
      case "publish_confirm":
        this.decoder.on("publish_confirm", listener as PublishConfirmListener)
        break
      case "publish_error":
        this.decoder.on("publish_error", listener as PublishErrorListener)
        break
      case "deliverV1":
        this.decoder.on("deliverV1", listener as DeliverListener)
        break
      case "deliverV2":
        this.decoder.on("deliverV2", listener as DeliverV2Listener)
        break
      case "consumer_update_query":
        this.decoder.on("consumer_update_query", listener as ConsumerUpdateQueryListener)
        break
      default:
        break
    }
  }

  private registerListeners(listeners?: ConnectionListenersParams) {
    if (listeners?.metadata_update) this.decoder.on("metadata_update", listeners.metadata_update)
    if (listeners?.publish_confirm) this.decoder.on("publish_confirm", listeners.publish_confirm)
    if (listeners?.publish_error) this.decoder.on("publish_error", listeners.publish_error)
    if (listeners?.deliverV1) this.decoder.on("deliverV1", listeners.deliverV1)
    if (listeners?.deliverV2) this.decoder.on("deliverV2", listeners.deliverV2)
    if (listeners?.consumer_update_query) this.decoder.on("consumer_update_query", listeners.consumer_update_query)
  }

  getCompression(compressionType: CompressionType) {
    const compression = this.compressions.get(compressionType)
    if (!compression) {
      throw new Error(
        "invalid compression or compression not yet implemented, to add a new compression use the specific api"
      )
    }

    return compression
  }

  registerCompression(compression: Compression) {
    const c = this.compressions.get(compression.getType())
    if (c) {
      throw new Error("compression already implemented")
    }
    this.compressions.set(compression.getType(), compression)
  }

  private async exchangeCommandVersions() {
    const versions = getClientSupportedVersions(this.peerProperties.version)
    const response = await this.sendAndWait<ExchangeCommandVersionsResponse>(
      new ExchangeCommandVersionsRequest(versions)
    )
    this.serverDeclaredVersions.push(...response.serverDeclaredVersions)

    checkServerDeclaredVersions(this.serverVersions, this.logger, this.peerProperties.version)
    return response
  }

  public sendAndWait<T extends Response>(cmd: Request): Promise<T> {
    return new Promise((res, rej) => {
      const correlationId = this.incCorrelationId()
      const bufferSizeParams = this.getBufferSizeParams()
      const body = cmd.toBuffer(bufferSizeParams, correlationId)
      this.logger.debug(
        `Write cmd key: ${cmd.key.toString(16)} - correlationId: ${correlationId}: data: ${inspect(
          body.toJSON()
        )} length: ${body.byteLength}`
      )
      this.socket.write(body, (err) => {
        this.logger.debug(
          `Write COMPLETED for cmd key: ${cmd.key.toString(16)} - correlationId: ${correlationId} err: ${err}`
        )
        if (err) {
          return rej(err)
        }
        this?.heartbeat?.reportLastMessageSent()
        res(this.waitResponse<T>({ correlationId, key: cmd.responseKey }))
      })
    })
  }

  private waitResponse<T extends Response>({ correlationId, key }: { correlationId: number; key: number }): Promise<T> {
    const response = removeFrom(this.receivedResponses, (r) => r.correlationId === correlationId)
    if (response) {
      if (response.key !== key) {
        throw new Error(
          `Error con correlationId: ${correlationId} waiting key: ${key.toString(
            16
          )} found key: ${response.key.toString(16)} `
        )
      }
      return response.ok ? Promise.resolve(response as T) : Promise.reject(response.code)
    }
    return new Promise((resolve, reject) => {
      this.waitingResponses.push(new WaitingResponse<T>(correlationId, key, { resolve, reject }))
    })
  }

  public getConnectionInfo(): ConnectionInfo {
    return {
      host: this.serverEndpoint.host,
      port: this.serverEndpoint.port,
      id: this.connectionId,
      readable: this.socket.readable,
      writable: this.socket.writable,
      localPort: this.socket.localPort,
    }
  }

  private responseReceived<T extends Response>(response: T) {
    const wr = removeFrom(this.waitingResponses as WaitingResponse<T>[], (x) => x.waitingFor(response))
    return wr ? wr.resolve(response) : this.receivedResponses.push(response)
  }

  private received(data: Buffer) {
    this.logger.debug(`Receiving ${data.length} (${data.readUInt32BE()}) bytes ... ${inspect(data)}`)
    this.decoder.add(data, (ct) => this.getCompression(ct))
  }

  private async exchangeProperties(): Promise<PeerPropertiesResponse> {
    this.logger.debug(`Exchange peer properties ...`)
    const peerProperties = {
      ...PEER_PROPERTIES,
      connection_name: this.params.connectionName ?? PEER_PROPERTIES.connection_name,
    }
    const res = await this.sendAndWait<PeerPropertiesResponse>(new PeerPropertiesRequest(peerProperties))
    if (!res.ok) {
      throw new Error(`Unable to exchange peer properties ${res.code} `)
    }
    this.logger.debug(`server properties: ${inspect(res.properties)}`)
    return res
  }

  public send(cmd: Request): Promise<void> {
    return new Promise((res, rej) => {
      const bufferSizeParams = this.getBufferSizeParams()
      const body = cmd.toBuffer(bufferSizeParams)
      this.logger.debug(
        `Write cmd key: ${cmd.key.toString(16)} - no correlationId - data: ${inspect(body.toJSON())} length: ${
          body.byteLength
        }`
      )
      this.socket.write(body, (err) => {
        this.logger.debug(`Write COMPLETED for cmd key: ${cmd.key.toString(16)} - no correlationId - err: ${err}`)
        if (err) {
          return rej(err)
        }
        return res()
      })
    })
  }

  private incCorrelationId() {
    this.correlationId += 1
    return this.correlationId
  }

  private getBufferSizeParams(): BufferSizeParams {
    return { maxSize: this.frameMax, ...this.bufferSizeSettings }
  }

  public get maxFrameSize() {
    return this.frameMax
  }

  public get serverVersions() {
    return [...this.serverDeclaredVersions]
  }

  public get rabbitManagementVersion() {
    return this.peerProperties.version
  }

  public get isFilteringEnabled() {
    return this.filteringEnabled
  }

  private async auth(params: { username: string; password: string }) {
    this.logger.debug(`Start authentication process ...`)
    this.logger.debug(`Start SASL handshake ...`)
    const handshakeResponse = await this.sendAndWait<SaslHandshakeResponse>(new SaslHandshakeRequest())
    this.logger.debug(`Mechanisms: ${handshakeResponse.mechanisms}`)
    if (!handshakeResponse.mechanisms.find((m) => m === "PLAIN")) {
      throw new Error(`Unable to find PLAIN mechanism in ${handshakeResponse.mechanisms}`)
    }

    this.logger.debug(`Start SASL PLAIN authentication ...`)
    const authResponse = await this.sendAndWait<SaslAuthenticateResponse>(
      new SaslAuthenticateRequest({ ...params, mechanism: "PLAIN" })
    )
    this.logger.debug(`Authentication: ${authResponse.ok} - '${authResponse.data}'`)
    if (!authResponse.ok) {
      throw new Error(`Unable Authenticate -> ${authResponse.code}`)
    }

    return authResponse
  }

  private async open(params: { virtualHost: string }) {
    this.logger.debug(`Open ...`)
    const res = await this.sendAndWait<OpenResponse>(new OpenRequest(params))
    this.logger.debug(`Open response: ${res.ok} - '${inspect(res.properties)}'`)
    const advertisedHost = res.properties["advertised_host"] ?? ""
    const advertisedPort = parseInt(res.properties["advertised_port"] ?? "5552")
    this.serverEndpoint = { host: advertisedHost, port: advertisedPort }
    return res
  }

  private async tune(heartbeatInterval: number): Promise<{ heartbeat: number }> {
    const tuneResponse = await this.waitResponse<TuneResponse>({ correlationId: -1, key: TuneResponse.key })
    this.logger.debug(`TUNE response -> ${inspect(tuneResponse)}`)
    const heartbeat = extractHeartbeatInterval(heartbeatInterval, tuneResponse)

    return new Promise((res, rej) => {
      this.frameMax = this.calculateFrameMaxSizeFrom(tuneResponse.frameMax)
      const request = new TuneRequest({ frameMax: this.frameMax, heartbeat })
      this.socket.write(request.toBuffer(), (err) => {
        this.logger.debug(`Write COMPLETED for cmd TUNE: ${inspect(tuneResponse)} - err: ${err}`)
        return err ? rej(err) : res({ heartbeat })
      })
    })
  }

  private calculateFrameMaxSizeFrom(tuneResponseFrameMax: number) {
    if (this.frameMax === DEFAULT_UNLIMITED_FRAME_MAX) return tuneResponseFrameMax
    if (tuneResponseFrameMax === DEFAULT_UNLIMITED_FRAME_MAX) return this.frameMax
    return Math.min(this.frameMax, tuneResponseFrameMax)
  }

  public async close(params: ClosingParams = { closingCode: 0, closingReason: "" }): Promise<void> {
    this.logger.info(`Closing client...`)
    this.logger.info(`Stopping heartbeat...`)
    this.heartbeat.stop()
    this.logger.debug(`Close...`)
    const closeResponse = await this.sendAndWait<CloseResponse>(new CloseRequest(params))
    this.logger.debug(`Close response: ${closeResponse.ok} - '${inspect(closeResponse)}'`)
    this.socket.end()
  }

  public async queryPublisherSequence(params: { stream: string; publisherRef: string }): Promise<bigint> {
    const res = await this.sendAndWait<QueryPublisherResponse>(new QueryPublisherRequest(params))
    if (!res.ok) {
      throw new Error(
        `Query Publisher Sequence command returned error with code ${res.code} - ${errorMessageOf(res.code)}`
      )
    }

    this.logger.info(
      `Sequence for stream name ${params.stream}, publisher ref ${params.publisherRef} at ${res.sequence}`
    )
    return res.sequence
  }

  public storeOffset(params: StoreOffsetParams): Promise<void> {
    return this.send(new StoreOffsetRequest(params))
  }

  public async queryOffset(params: QueryOffsetParams): Promise<bigint> {
    this.logger.debug(`Query Offset...`)
    const res = await this.sendAndWait<QueryOffsetResponse>(new QueryOffsetRequest(params))
    if (!res.ok) {
      throw new Error(`Query offset command returned error with code ${res.code}`)
    }
    this.logger.debug(`Query Offset response: ${res.ok} with params: '${inspect(params)}'`)
    return res.offsetValue
  }

  public incrRefCount() {
    ++this.refs
  }

  public decrRefCount() {
    return --this.refs
  }

  public get refCount() {
    return this.refs
  }
}

export function errorMessageOf(code: number): string {
  switch (code) {
    case 0x02:
      return "Stream does not exist"
    case 0x06:
      return "Stream not available"
    case 0x12:
      return "Publisher does not exist"
    default:
      return "Unknown error"
  }
}

export function connect(logger: Logger, params: ConnectionParams) {
  return Connection.connect(params, logger)
}

export function create(logger: Logger, params: ConnectionParams) {
  return Connection.create(params, logger)
}
