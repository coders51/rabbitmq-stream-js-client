import { Socket } from "net"
import { inspect } from "util"
import { STREAM_ALREADY_EXISTS_ERROR_CODE } from "./error_codes"
import { Heartbeat } from "./heartbeat"
import { Producer } from "./producer"
import { CloseRequest } from "./requests/close_request"
import { CreateStreamArguments, CreateStreamRequest } from "./requests/create_stream_request"
import { DeclarePublisherRequest } from "./requests/declare_publisher_request"
import { DeleteStreamRequest } from "./requests/delete_stream_request"
import { OpenRequest } from "./requests/open_request"
import { PeerPropertiesRequest } from "./requests/peer_properties_request"
import { QueryPublisherRequest } from "./requests/query_publisher_request"
import { Request } from "./requests/request"
import { SaslAuthenticateRequest } from "./requests/sasl_authenticate_request"
import { SaslHandshakeRequest } from "./requests/sasl_handshake_request"
import { TuneRequest } from "./requests/tune_request"
import { CloseResponse } from "./responses/close_response"
import { CreateStreamResponse } from "./responses/create_stream_response"
import { DeclarePublisherResponse } from "./responses/declare_publisher_response"
import { DeletePublisherRequest } from "./requests/delete_publisher_request"
import { DeletePublisherResponse } from "./responses/delete_publisher_response"
import { DeleteStreamResponse } from "./responses/delete_stream_response"
import { QueryPublisherResponse } from "./responses/query_publisher_response"
import { PeerPropertiesResponse } from "./responses/peer_properties_response"
import { OpenResponse } from "./responses/open_response"
import { Response } from "./responses/response"
import {
  MetadataUpdateListener,
  PublishConfirmListener,
  PublishErrorListener,
  ResponseDecoder,
} from "./response_decoder"
import { createConsoleLog, removeFrom } from "./util"
import { WaitingResponse } from "./waiting_response"
import { SubscribeResponse } from "./responses/subscribe_response"
import { TuneResponse } from "./responses/tune_response"
import { SaslHandshakeResponse } from "./responses/sasl_handshake_response"
import { SaslAuthenticateResponse } from "./responses/sasl_authenticate_response"
import { Offset, SubscribeRequest } from "./requests/subscribe_request"
import { Consumer, ConsumerFunc } from "./consumer"
import { UnsubscribeResponse } from "./responses/unsubscribe_response"
import { UnsubscribeRequest } from "./requests/unsubscribe_request"
import { CreditRequest, CreditRequestParams } from "./requests/credit_request"
import { StreamStatsRequest } from "./requests/stream_stats_request"
import { StreamStatsResponse } from "./responses/stream_stats_response"
import { StoreOffsetRequest } from "./requests/store_offset_request"
import { DeliverResponse } from "./responses/deliver_response"
export class Connection {
  private readonly socket = new Socket()
  private readonly logger = createConsoleLog()
  private correlationId = 100
  private decoder: ResponseDecoder
  private receivedResponses: Response[] = []
  private waitingResponses: WaitingResponse<never>[] = []
  private publisherId = 0
  private heartbeat: Heartbeat
  private consumerId = 0
  private consumers = new Map<number, Consumer>()

  constructor() {
    this.heartbeat = new Heartbeat(this, this.logger)
    this.decoder = new ResponseDecoder((...args) => this.responseReceived(...args), this.logger)
  }

  static connect(params: ConnectionParams): Promise<Connection> {
    return new Connection().start(params)
  }

  public start(params: ConnectionParams): Promise<Connection> {
    this.registerListeners(params.listeners)
    this.registerDelivers()
    return new Promise((res, rej) => {
      this.socket.on("error", (err) => {
        this.logger.warn(`Error on connection ${params.hostname}:${params.port} vhost:${params.vhost} err: ${err}`)
        return rej(err)
      })
      this.socket.on("connect", async () => {
        this.logger.info(`Connected to RabbitMQ ${params.hostname}:${params.port}`)
        await this.exchangeProperties()
        await this.auth({ username: params.username, password: params.password })
        const { heartbeat } = await this.tune(params.heartbeat ?? 0)
        await this.open({ virtualHost: params.vhost })
        this.heartbeat.start(heartbeat)
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
  public on(event: "metadata_update", listener: MetadataUpdateListener): void
  public on(event: "publish_confirm", listener: PublishConfirmListener): void
  public on(event: "publish_error", listener: PublishErrorListener): void
  public on(
    event: "metadata_update" | "publish_confirm" | "publish_error",
    listener: MetadataUpdateListener | PublishConfirmListener | PublishErrorListener
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
      default:
        break
    }
  }

  public async close(
    params: { closingCode: number; closingReason: string } = { closingCode: 0, closingReason: "" }
  ): Promise<void> {
    this.logger.info(`Closing connection ...`)
    this.heartbeat.stop()
    this.logger.debug(`Close ...`)
    const closeResponse = await this.sendAndWait<CloseResponse>(new CloseRequest(params))
    this.logger.debug(`Close response: ${closeResponse.ok} - '${inspect(closeResponse)}'`)
    return new Promise((res, _rej) => this.socket.end(() => res()))
  }

  public async declarePublisher(params: DeclarePublisherParams): Promise<Producer> {
    const { stream, publisherRef } = params
    const publisherId = this.incPublisherId()
    const res = await this.sendAndWait<DeclarePublisherResponse>(
      new DeclarePublisherRequest({ stream, publisherRef, publisherId })
    )
    if (!res.ok) {
      throw new Error(`Declare Publisher command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }

    const producer = new Producer({
      connection: this,
      stream: params.stream,
      publisherId: publisherId,
      publisherRef: params.publisherRef,
      boot: params.boot,
    })
    this.logger.info(
      `New producer created with stream name ${params.stream}, publisher id ${publisherId} and publisher reference ${params.publisherRef}`
    )

    return producer
  }

  public async deletePublisher(publisherId: number) {
    const res = await this.sendAndWait<DeletePublisherResponse>(new DeletePublisherRequest(publisherId))
    if (!res.ok) {
      throw new Error(`Delete Publisher command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`deleted producer with publishing id ${publisherId}`)
    return res.ok
  }

  public async declareConsumer(params: DeclareConsumerParams, handle: ConsumerFunc): Promise<Consumer> {
    const consumerId = this.incConsumerId()
    const consumer = new Consumer(handle, {
      connection: this,
      stream: params.stream,
      consumerId,
      consumerRef: params.consumerRef,
    })
    this.consumers.set(consumerId, consumer)

    const res = await this.sendAndWait<SubscribeResponse>(
      new SubscribeRequest({ ...params, subscriptionId: consumerId, credit: 10 })
    )
    if (!res.ok) {
      this.consumers.delete(consumerId)
      throw new Error(`Declare Consumer command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }

    this.logger.info(
      `New consumer created with stream name ${
        params.stream
      }, consumer id ${consumerId} and offset ${params.offset.toString()}`
    )
    return consumer
  }

  public async closeConsumer(consumerId: number) {
    const consumer = this.consumers.get(consumerId)
    if (!consumer) {
      this.logger.error("Consumer does not exist")
      throw new Error(`Consumer with id: ${consumerId} does not exist`)
    }
    const res = await this.sendAndWait<UnsubscribeResponse>(new UnsubscribeRequest(consumerId))
    if (!res.ok) {
      throw new Error(`Unsubscribe command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.consumers.delete(consumerId)
    this.logger.info(`Closed consumer with id: ${consumerId}`)
    return res.ok
  }

  public consumerCounts() {
    return this.consumers.size
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

  public async createStream(params: { stream: string; arguments: CreateStreamArguments }): Promise<true> {
    this.logger.debug(`Create Stream...`)
    const res = await this.sendAndWait<CreateStreamResponse>(new CreateStreamRequest(params))
    if (res.code === STREAM_ALREADY_EXISTS_ERROR_CODE) {
      return true
    }
    if (!res.ok) {
      throw new Error(`Create Stream command returned error with code ${res.code}`)
    }

    this.logger.debug(`Create Stream response: ${res.ok} - with arguments: '${inspect(params.arguments)}'`)
    return res.ok
  }

  public async deleteStream(params: { stream: string }): Promise<true> {
    this.logger.debug(`Delete Stream...`)
    const res = await this.sendAndWait<DeleteStreamResponse>(new DeleteStreamRequest(params.stream))
    if (!res.ok) {
      throw new Error(`Delete Stream command returned error with code ${res.code}`)
    }
    this.logger.debug(`Delete Stream response: ${res.ok} - '${inspect(params.stream)}'`)
    return res.ok
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

  public async streamStatsRequest(streamName: string) {
    const res = await this.sendAndWait<StreamStatsResponse>(new StreamStatsRequest(streamName))
    if (!res.ok) {
      throw new Error(`Stream Stats command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Statistics for stream name ${streamName}, ${res.statistics}`)
    return res.statistics
  }

  private responseReceived<T extends Response>(response: T) {
    const wr = removeFrom(this.waitingResponses as WaitingResponse<T>[], (x) => x.waitingFor(response))
    return wr ? wr.resolve(response) : this.receivedResponses.push(response)
  }

  private received(data: Buffer) {
    this.logger.debug(`Receiving ${data.length} (${data.readUInt32BE()}) bytes ... ${inspect(data)}`)
    this.decoder.add(data)
  }

  private async tune(heartbeatInterval: number): Promise<{ heartbeat: number }> {
    const tuneResponse = await this.waitResponse<TuneResponse>({ correlationId: -1, key: TuneResponse.key })
    this.logger.debug(`TUNE response -> ${inspect(tuneResponse)}`)
    const heartbeat = extractHeartbeatInterval(heartbeatInterval, tuneResponse)

    return new Promise((res, rej) => {
      const request = new TuneRequest({ frameMax: tuneResponse.frameMax, heartbeat })
      this.socket.write(request.toBuffer(), (err) => {
        this.logger.debug(`Write COMPLETED for cmd TUNE: ${inspect(tuneResponse)} - err: ${err}`)
        return err ? rej(err) : res({ heartbeat })
      })
    })
  }

  public async subscribe(params: SubscribeParams): Promise<SubscribeResponse> {
    const res = await this.sendAndWait<SubscribeResponse>(new SubscribeRequest({ ...params }))
    if (!res.ok) {
      throw new Error(`Subscribe command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    return res
  }

  private askForCredit(params: CreditRequestParams): Promise<void> {
    return this.send(new CreditRequest({ ...params }))
  }

  public storeOffset(params: StoreOffsetParams): Promise<void> {
    return this.send(new StoreOffsetRequest(params))
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

  private incConsumerId() {
    const consumerId = this.consumerId
    this.consumerId++
    return consumerId
  }

  private registerListeners(listeners?: ListenersParams) {
    if (listeners?.metadata_update) this.decoder.on("metadata_update", listeners.metadata_update)
    if (listeners?.publish_confirm) this.decoder.on("publish_confirm", listeners.publish_confirm)
    if (listeners?.publish_error) this.decoder.on("publish_error", listeners.publish_error)
  }

  private registerDelivers() {
    this.decoder.on("deliver", async (response: DeliverResponse) => {
      const consumer = this.consumers.get(response.subscriptionId)
      if (!consumer) {
        this.logger.error(`On deliver no consumer found`)
        return
      }
      this.logger.debug(`on deliver -> ${consumer.consumerId}`)
      this.logger.debug(`response.messages.length: ${response.messages.length}`)
      await this.askForCredit({ credit: 1, subscriptionId: response.subscriptionId })
      response.messages.map((x) => consumer.handle(x))
    })
  }
}

export type ListenersParams = {
  metadata_update?: MetadataUpdateListener
  publish_confirm?: PublishConfirmListener
  publish_error?: PublishErrorListener
}

export interface ConnectionParams {
  hostname: string
  port: number
  username: string
  password: string
  vhost: string
  frameMax?: number // not used
  heartbeat?: number
  listeners?: ListenersParams
}

export interface DeclarePublisherParams {
  stream: string
  publisherRef?: string
  boot?: boolean
}

export interface DeclareConsumerParams {
  stream: string
  consumerRef?: string
  offset: Offset
}

export interface SubscribeParams {
  subscriptionId: number
  stream: string
  credit: number
  offset: Offset
}

export interface StoreOffsetParams {
  reference: string
  stream: string
  offsetValue: bigint
}

export function connect(params: ConnectionParams): Promise<Connection> {
  return Connection.connect(params)
}

function errorMessageOf(code: number): string {
  switch (code) {
    case 0x02:
      return "Stream does not exist"
    case 0x12:
      return "Publisher does not exist"

    default:
      return "Unknown error"
  }
}

function extractHeartbeatInterval(heartbeatInterval: number, tuneResponse: TuneResponse): number {
  return heartbeatInterval === 0 ? tuneResponse.heartbeat : Math.min(heartbeatInterval, tuneResponse.heartbeat)
}
