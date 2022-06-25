import { Socket } from "net"
import { inspect } from "util"
import { Command } from "./command"
import { PeerPropertiesRequest, SaslHandshakeRequest } from "./peer_properties_request"
import { PeerPropertiesResponse, SaslHandshakeResponse } from "./peer_properties_response"
import { Response } from "./response"
import { ResponseDecoder } from "./response_decoder"
import { createConsoleLog, removeFrom } from "./util"
import { WaitingResponse } from "./waiting_response"

export class Connection {
  private readonly socket = new Socket()
  private readonly logger = createConsoleLog()
  private correlationId = 100
  private decoder: ResponseDecoder
  private receivedResponses: Response[] = []
  private waitingResponses: WaitingResponse<never>[] = []

  constructor() {
    this.decoder = new ResponseDecoder(this)
  }

  static connect(params: ConnectionParams): Promise<Connection> {
    const c = new Connection()
    return c.start(params)
  }

  responseReceived<T extends Response>(response: T) {
    const wr = removeFrom(this.waitingResponses as WaitingResponse<T>[], (x) => x.waitingFor(response))
    return wr ? wr.resolve(response) : this.receivedResponses.push(response)
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
        this.received(data)
      })
      this.socket.on("close", (had_error) => {
        this.logger.info(`Close event on socket, close cloud had_error? ${had_error}`)
      })
      this.socket.connect(params.port, params.hostname)
    })
  }
  private received(data: Buffer) {
    this.logger.debug(`Receiving data ... ${inspect(data)}`)
    this.decoder.add(data)
  }

  open() {
    throw new Error("Method not implemented.")
  }
  tune() {
    throw new Error("Method not implemented.")
  }

  async exchangeProperties(): Promise<PeerPropertiesResponse> {
    this.logger.debug(`Exchange properties ...`)
    const req = new PeerPropertiesRequest()
    const res = await this.SendAndWait<PeerPropertiesResponse>(req)
    this.logger.debug(`SendAndWait... return: ${inspect(res)}`) // TODO -> print properties
    return res
  }

  async auth() {
    this.logger.debug(`auth ...`)
    const handshakeResponse = await this.SendAndWait<SaslHandshakeResponse>(new SaslHandshakeRequest())
    this.logger.debug(`Mechanisms: ${handshakeResponse.mechanisms}`)
    if (!handshakeResponse.mechanisms.find((m) => m === "PLAIN")) {
      throw new Error(`Unable to find PLAIN mechanism in ${handshakeResponse.mechanisms}`)
    }

    return handshakeResponse

    /**
 *             var saslHandshakeResponse =
                await client.Request<SaslHandshakeRequest, SaslHandshakeResponse>(
                    corr => new SaslHandshakeRequest(corr));
            foreach (var m in saslHandshakeResponse.Mechanisms)
            {
                Debug.WriteLine($"sasl mechanism: {m}");
            }

            var saslData = Encoding.UTF8.GetBytes($"\0{parameters.UserName}\0{parameters.Password}");
            var authResponse =
                await client.Request<SaslAuthenticateRequest, SaslAuthenticateResponse>(corr =>
                    new SaslAuthenticateRequest(corr, "PLAIN", saslData));
            Debug.WriteLine($"auth: {authResponse.ResponseCode} {authResponse.Data}");
            ClientExceptions.MaybeThrowException(authResponse.ResponseCode, parameters.UserName);

 */
  }

  SendAndWait<T extends Response>(cmd: Command): Promise<T> {
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
        res(this.waitResponse<T>({ correlationId, key: cmd.responseKey }))
      })
    })
  }

  waitResponse<T extends Response>({ correlationId, key }: { correlationId: number; key: number }): Promise<T> {
    const response = removeFrom(this.receivedResponses, (r) => r.correlationId === correlationId)
    if (response) {
      if (response.key !== key) {
        throw new Error(`Error con correlationId: ${correlationId} waiting key: ${key} found key: ${response.key} `)
      }
      return response.ok ? Promise.resolve(response as T) : Promise.reject(response.code)
    }
    return new Promise((resolve, reject) => {
      this.waitingResponses.push(new WaitingResponse<T>(correlationId, key, { resolve, reject }))
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
