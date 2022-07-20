import { Socket } from "net"
import { inspect } from "util"
import { OpenRequest } from "./requests/open_request"
import { PeerPropertiesRequest } from "./requests/peer_properties_request"
import { Request } from "./requests/request"
import { SaslAuthenticateRequest } from "./requests/sasl_authenticate_request"
import { SaslHandshakeRequest } from "./requests/sasl_handshake_request"
import { PeerPropertiesResponse } from "./responses/peer_properties_response"
import { OpenResponse } from "./responses/open_response"
import { SaslHandshakeResponse } from "./responses/sasl_handshake_response"
import { SaslAuthenticateResponse } from "./responses/sasl_authenticate_response"
import { Response } from "./responses/response"
import { ResponseDecoder } from "./response_decoder"
import { createConsoleLog, removeFrom } from "./util"
import { WaitingResponse } from "./waiting_response"
import { TuneResponse } from "./responses/tune_response"
import { Producer } from "./producer"
import { DeclarePublisherResponse } from "./responses/declare_publisher_response"
import { DeclarePublisherRequest } from "./requests/declare_publisher_request"
import { CreateStreamResponse } from "./responses/create_stream_response"
import { CreateStreamRequest, CreateStreamArguments } from "./requests/create_stream_request"
import { Heartbeat } from "./heartbeat"
import { TuneRequest } from "./requests/tune_request"
import { StreamAlreadyExistsErrorCode } from "./error_codes"

export class Connection {
  private readonly socket = new Socket()
  private readonly logger = createConsoleLog()
  private correlationId = 100
  private decoder: ResponseDecoder
  private receivedResponses: Response[] = []
  private waitingResponses: WaitingResponse<never>[] = []
  private publisherId = 0
  private heartbeat: Heartbeat
  private heartbeatInterval = 0

  constructor() {
    this.heartbeat = new Heartbeat(this, this.logger)
    this.decoder = new ResponseDecoder((...args) => this.responseReceived(...args), this.logger)
  }

  static connect(params: ConnectionParams): Promise<Connection> {
    return new Connection().start(params)
  }

  public start(params: ConnectionParams): Promise<Connection> {
    this.heartbeatInterval = params.heartbeat || 0
    return new Promise((res, rej) => {
      this.socket.on("error", (err) => {
        this.logger.warn(`Error on connection ${params.hostname}:${params.port} vhost:${params.vhost} err: ${err}`)
        return rej(err)
      })
      this.socket.on("connect", async () => {
        this.logger.info(`Connected to RabbitMQ ${params.hostname}:${params.port}`)
        await this.exchangeProperties()
        await this.auth({ username: params.username, password: params.password })
        await this.tune()
        await this.open({ virtualHost: params.vhost })
        this.heartbeat.start(this.heartbeatInterval)
        return res(this)
      })
      this.socket.on("drain", () => this.logger.warn(`Draining ${params.hostname}:${params.port}`))
      this.socket.on("timeout", () => {
        this.logger.error(`Timeout ${params.hostname}:${params.port}`)
        return rej()
      })
      this.socket.on("data", (data) => {
        this.heartbeat.reportLastMessageReceived()
        this.received(data)
      })
      this.socket.on("close", (had_error) => {
        this.logger.info(`Close event on socket, close cloud had_error? ${had_error}`)
      })
      this.socket.connect(params.port, params.hostname)
    })
  }

  public close(): Promise<void> {
    this.logger.info(`Closing connection ...`)
    return new Promise((res, _rej) => this.socket.end(() => res()))
  }

  public async declarePublisher(params: DeclarePublisherParams): Promise<Producer> {
    const publisherId = this.incPublisherId()
    const res = await this.sendAndWait<DeclarePublisherResponse>(
      new DeclarePublisherRequest({ ...params, publisherId })
    )
    if (!res.ok) {
      throw new Error(`Declare Publisher command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }

    const producer = new Producer(this, publisherId)
    this.logger.info(
      `New producer created with stream name ${params.stream}, publisher id ${publisherId} and publisher reference ${params.publisherRef}`
    )
    return producer
  }

  public send(cmd: Request): Promise<void> {
    return new Promise((res, rej) => {
      const body = cmd.toBuffer()
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

  private responseReceived<T extends Response>(response: T) {
    const wr = removeFrom(this.waitingResponses as WaitingResponse<T>[], (x) => x.waitingFor(response))
    return wr ? wr.resolve(response) : this.receivedResponses.push(response)
  }

  private received(data: Buffer) {
    this.logger.debug(`Receiving ${data.length} (${data.readUInt32BE()}) bytes ... ${inspect(data)}`)
    this.decoder.add(data)
  }

  private async tune(): Promise<void> {
    const tuneResponse = await this.waitResponse<TuneResponse>({ correlationId: -1, key: TuneResponse.key })
    this.logger.debug(`TUNE response -> ${inspect(tuneResponse)}`)
    this.heartbeatInterval = this.extractHeartbeatInterval(tuneResponse)

    return new Promise((res, rej) => {
      const request = new TuneRequest({ frameMax: tuneResponse.frameMax, heartbeat: this.heartbeatInterval })
      this.socket.write(request.toBuffer(), (err) => {
        this.logger.debug(`Write COMPLETED for cmd TUNE: ${inspect(tuneResponse)} - err: ${err}`)
        return err ? rej(err) : res()
      })
    })
  }

  private extractHeartbeatInterval(tuneResponse: TuneResponse) {
    return this.heartbeatInterval === 0
      ? tuneResponse.heartbeat
      : Math.min(this.heartbeatInterval, tuneResponse.heartbeat)
  }

  private async exchangeProperties(): Promise<PeerPropertiesResponse> {
    this.logger.debug(`Exchange peer properties ...`)
    const res = await this.sendAndWait<PeerPropertiesResponse>(new PeerPropertiesRequest())
    if (!res.ok) {
      throw new Error(`Unable to exchange peer properties ${res.code} `)
    }
    this.logger.debug(`server properties: ${inspect(res.properties)}`)
    return res
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
    return res
  }

  async createStream(params: { stream: string; arguments: CreateStreamArguments }): Promise<true> {
    this.logger.debug(`Create Stream...`)
    const res = await this.sendAndWait<CreateStreamResponse>(new CreateStreamRequest(params))
    if (res.code === StreamAlreadyExistsErrorCode) {
      return true
    }
    if (!res.ok) {
      throw new Error(`Create Stream command returned error with code ${res.code}`)
    }

    this.logger.debug(`Create Stream response: ${res.ok} - with arguments: '${inspect(params.arguments)}'`)
    return res.ok
  }

  private sendAndWait<T extends Response>(cmd: Request): Promise<T> {
    return new Promise((res, rej) => {
      const correlationId = this.incCorrelationId()
      const body = cmd.toBuffer(correlationId)
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

  private incCorrelationId() {
    this.correlationId += 1
    return this.correlationId
  }

  private incPublisherId() {
    const publisherId = this.publisherId
    this.publisherId++
    return publisherId
  }
}

export interface ConnectionParams {
  hostname: string
  port: number
  username: string
  password: string
  vhost: string
  frameMax?: number // not used
  heartbeat?: number
}

export interface DeclarePublisherParams {
  stream: string
  publisherRef?: string
}

export function connect(params: ConnectionParams): Promise<Connection> {
  return Connection.connect(params)
}

function errorMessageOf(code: number): string {
  switch (code) {
    case 0x02:
      return "Stream does not exist"

    default:
      return "Unknown error"
  }
}
