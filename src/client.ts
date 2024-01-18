import { randomUUID } from "crypto"
import { Socket } from "net"
import tls from "node:tls"
import { inspect } from "util"
import { Compression, CompressionType, GzipCompression, NoneCompression } from "./compression"
import { Consumer, ConsumerFunc, StreamConsumer } from "./consumer"
import { STREAM_ALREADY_EXISTS_ERROR_CODE } from "./error_codes"
import { Heartbeat } from "./heartbeat"
import { Logger, NullLogger } from "./logger"
import { Message, Publisher, StreamPublisher } from "./publisher"
import { CloseRequest } from "./requests/close_request"
import { ConsumerUpdateResponse } from "./requests/consumer_update_response"
import { CreateStreamArguments, CreateStreamRequest } from "./requests/create_stream_request"
import { CreditRequest, CreditRequestParams } from "./requests/credit_request"
import { DeclarePublisherRequest } from "./requests/declare_publisher_request"
import { DeletePublisherRequest } from "./requests/delete_publisher_request"
import { DeleteStreamRequest } from "./requests/delete_stream_request"
import { ExchangeCommandVersionsRequest } from "./requests/exchange_command_versions_request"
import { MetadataRequest } from "./requests/metadata_request"
import { OpenRequest } from "./requests/open_request"
import { PartitionsQuery } from "./requests/partitions_query"
import { PeerPropertiesRequest } from "./requests/peer_properties_request"
import { QueryOffsetRequest } from "./requests/query_offset_request"
import { QueryPublisherRequest } from "./requests/query_publisher_request"
import { BufferSizeParams, BufferSizeSettings, Request } from "./requests/request"
import { RouteQuery } from "./requests/route_query"
import { SaslAuthenticateRequest } from "./requests/sasl_authenticate_request"
import { SaslHandshakeRequest } from "./requests/sasl_handshake_request"
import { StoreOffsetRequest } from "./requests/store_offset_request"
import { StreamStatsRequest } from "./requests/stream_stats_request"
import { Offset, SubscribeRequest } from "./requests/subscribe_request"
import { TuneRequest } from "./requests/tune_request"
import { UnsubscribeRequest } from "./requests/unsubscribe_request"
import {
  MetadataUpdateListener,
  PublishConfirmListener,
  PublishErrorListener,
  ResponseDecoder,
} from "./response_decoder"
import { CloseResponse } from "./responses/close_response"
import { ConsumerUpdateQuery } from "./responses/consumer_update_query"
import { CreateStreamResponse } from "./responses/create_stream_response"
import { DeclarePublisherResponse } from "./responses/declare_publisher_response"
import { DeletePublisherResponse } from "./responses/delete_publisher_response"
import { DeleteStreamResponse } from "./responses/delete_stream_response"
import { DeliverResponse } from "./responses/deliver_response"
import { ExchangeCommandVersionsResponse } from "./responses/exchange_command_versions_response"
import { Broker, MetadataResponse, StreamMetadata } from "./responses/metadata_response"
import { OpenResponse } from "./responses/open_response"
import { PartitionsResponse } from "./responses/partitions_response"
import { PeerPropertiesResponse } from "./responses/peer_properties_response"
import { QueryOffsetResponse } from "./responses/query_offset_response"
import { QueryPublisherResponse } from "./responses/query_publisher_response"
import { Response } from "./responses/response"
import { RouteResponse } from "./responses/route_response"
import { SaslAuthenticateResponse } from "./responses/sasl_authenticate_response"
import { SaslHandshakeResponse } from "./responses/sasl_handshake_response"
import { StreamStatsResponse } from "./responses/stream_stats_response"
import { SubscribeResponse } from "./responses/subscribe_response"
import { TuneResponse } from "./responses/tune_response"
import { UnsubscribeResponse } from "./responses/unsubscribe_response"
import { SuperStreamConsumer } from "./super_stream_consumer"
import {
  DEFAULT_FRAME_MAX,
  DEFAULT_UNLIMITED_FRAME_MAX,
  REQUIRED_MANAGEMENT_VERSION,
  getMaxSharedClientInstances,
  removeFrom,
  sample,
} from "./util"
import { Version, checkServerDeclaredVersions, getClientSupportedVersions } from "./versions"
import { WaitingResponse } from "./waiting_response"
import { CreateSuperStreamRequest } from "./requests/create_super_stream_request"
import { CreateSuperStreamResponse } from "./responses/create_super_stream_response"
import { DeleteSuperStreamResponse } from "./responses/delete_super_stream_response"
import { DeleteSuperStreamRequest } from "./requests/delete_super_stream_request"
import { lt, coerce } from "semver"

export type ConnectionClosedListener = (hadError: boolean) => void
export type ConnectionInfo = {
  host: string
  port: number
  id: string
  readable?: boolean
  writable?: boolean
  localPort?: number
}
type ClientInstanceKey = string

export class Client {
  private id: string = randomUUID()
  private socket: Socket
  private hostname: string
  private correlationId = 100
  private decoder: ResponseDecoder
  private receivedResponses: Response[] = []
  private waitingResponses: WaitingResponse<never>[] = []
  private publisherId = 0
  private heartbeat: Heartbeat
  private consumerId = 0
  private consumers = new Map<number, StreamConsumer>()
  private publishers = new Map<number, { client: Client; publisher: StreamPublisher }>()
  private compressions = new Map<CompressionType, Compression>()
  private readonly bufferSizeSettings: BufferSizeSettings
  private frameMax: number = DEFAULT_FRAME_MAX
  private connectionId: string
  private connectionClosedListener: ConnectionClosedListener | undefined
  private serverEndpoint: { host: string; port: number } = { host: "", port: 5552 }
  private readonly serverDeclaredVersions: Version[] = []
  private peerProperties: Record<string, string> = {}
  private static consumerClients = new Map<ClientInstanceKey, Client[]>()
  private static publisherClients = new Map<ClientInstanceKey, Client[]>()
  private refCount = 0
  private leader: boolean
  private streamName: string | undefined

  private constructor(
    private readonly logger: Logger,
    private readonly params: ConnectionParams,
  ) {
    if (params.frameMax) this.frameMax = params.frameMax
    if (params.ssl) {
      this.socket = tls.connect(params.port, params.hostname, { ...params.ssl, rejectUnauthorized: false })
    } else {
      this.socket = new Socket()
      this.socket.connect(this.params.port, this.params.hostname)
    }
    if (params.socketTimeout) this.socket.setTimeout(params.socketTimeout)
    this.hostname = params.hostname
    this.leader = params.leader ?? false
    this.streamName = params.streamName
    this.heartbeat = new Heartbeat(this, this.logger)
    this.compressions.set(CompressionType.None, NoneCompression.create())
    this.compressions.set(CompressionType.Gzip, GzipCompression.create())
    this.decoder = new ResponseDecoder((...args) => this.responseReceived(...args), this.logger)
    this.bufferSizeSettings = params.bufferSizeSettings || {}
    this.connectionId = randomUUID()
    this.connectionClosedListener = params.listeners?.connection_closed
  }

  getCompression(compressionType: CompressionType) {
    const compression = this.compressions.get(compressionType)
    if (!compression) {
      throw new Error(
        "invalid compression or compression not yet implemented, to add a new compression use the specific api",
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

  static async connect(params: ConnectionParams, logger?: Logger): Promise<Client> {
    return new Client(logger ?? new NullLogger(), params).start()
  }

  public start(): Promise<Client> {
    this.registerListeners(this.params.listeners)
    this.registerDelivers()
    this.registerConsumerUpdateQuery()

    return new Promise((res, rej) => {
      this.socket.on("error", (err) => {
        this.logger.warn(
          `Error on client ${this.params.hostname}:${this.params.port} vhost:${this.params.vhost} err: ${err}`,
        )
        return rej(err)
      })
      this.socket.on("connect", async () => {
        this.logger.info(`Connected to RabbitMQ ${this.params.hostname}:${this.params.port}`)
        this.peerProperties = (await this.exchangeProperties()).properties
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
  public on(
    event: "metadata_update" | "publish_confirm" | "publish_error",
    listener: MetadataUpdateListener | PublishConfirmListener | PublishErrorListener,
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
    params: { closingCode: number; closingReason: string } = { closingCode: 0, closingReason: "" },
  ): Promise<void> {
    this.logger.info(`${this.id} Closing client...`)
    const refs = this.decrRefCount()
    if (refs <= 0) {
      if (this.publisherCounts()) {
        this.logger.info(`Stopping all producers...`)
        await this.closeAllPublishers()
      }
      if (this.consumerCounts()) {
        this.logger.info(`Stopping all consumers...`)
        await this.closeAllConsumers()
      }
      this.logger.info(`Stopping heartbeat...`)
      this.heartbeat.stop()
      this.logger.debug(`Close...`)
      const closeResponse = await this.sendAndWait<CloseResponse>(new CloseRequest(params))
      this.logger.debug(`Close response: ${closeResponse.ok} - '${inspect(closeResponse)}'`)
      Client.removeCachedClient(this)
      this.socket.end()
    }
  }

  public async queryMetadata(params: QueryMetadataParams): Promise<StreamMetadata[]> {
    const { streams } = params
    const res = await this.sendAndWait<MetadataResponse>(new MetadataRequest({ streams }))
    if (!res.ok) {
      throw new Error(`Query Metadata command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Returned stream metadata for streams with names ${params.streams.join(",")}`)
    const { streamInfos } = res

    return streamInfos
  }

  public async queryPartitions(params: QueryPartitionsParams): Promise<string[]> {
    const { superStream } = params
    const res = await this.sendAndWait<PartitionsResponse>(new PartitionsQuery({ superStream }))
    if (!res.ok) {
      throw new Error(`Query Partitions command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Returned superstream partitions for superstream ${superStream}`)
    return res.streams
  }

  public async declarePublisher(params: DeclarePublisherParams): Promise<Publisher> {
    const { stream, publisherRef } = params
    const publisherId = this.incPublisherId()

    const client = await this.initNewClient(params.stream, true, params.connectionClosedListener)
    const res = await client.sendAndWait<DeclarePublisherResponse>(
      new DeclarePublisherRequest({ stream, publisherRef, publisherId }),
    )
    if (!res.ok) {
      await client.close()
      throw new Error(`Declare Publisher command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    const publisher = new StreamPublisher({
      client,
      stream: params.stream,
      publisherId: publisherId,
      publisherRef: params.publisherRef,
      boot: params.boot,
      maxFrameSize: this.frameMax,
      maxChunkLength: params.maxChunkLength,
      logger: this.logger,
    })
    this.publishers.set(publisherId, { publisher: publisher, client: client })
    this.logger.info(
      `New publisher created with stream name ${params.stream}, publisher id ${publisherId} and publisher reference ${params.publisherRef}`,
    )

    return publisher
  }

  public async deletePublisher(publisherId: number) {
    const publisherClient = this.publishers.get(publisherId)?.client ?? this
    const res = await publisherClient.sendAndWait<DeletePublisherResponse>(new DeletePublisherRequest(publisherId))
    if (!res.ok) {
      throw new Error(`Delete Publisher command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    await this.publishers.get(publisherId)?.publisher.close()
    this.publishers.delete(publisherId)
    this.logger.info(`deleted publisher with publishing id ${publisherId}`)
    return res.ok
  }

  public async declareConsumer(params: DeclareConsumerParams, handle: ConsumerFunc): Promise<Consumer> {
    const consumerId = this.incConsumerId()
    const properties: Record<string, string> = {}
    const client = await this.initNewClient(params.stream, false, params.connectionClosedListener)
    const consumer = new StreamConsumer(addOffsetFilterToHandle(handle, params.offset), {
      client,
      stream: params.stream,
      consumerId,
      consumerRef: params.consumerRef,
      offset: params.offset,
    })
    this.consumers.set(consumerId, consumer)

    if (params.singleActive && !params.consumerRef) {
      throw new Error("consumerRef is mandatory when declaring a single active consumer")
    }
    if (params.singleActive) {
      properties["single-active-consumer"] = "true"
      properties["name"] = params.consumerRef!
    }

    const res = await this.sendAndWait<SubscribeResponse>(
      new SubscribeRequest({ ...params, subscriptionId: consumerId, credit: 10, properties: properties }),
    )

    if (!res.ok) {
      this.consumers.delete(consumerId)
      throw new Error(`Declare Consumer command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }

    this.logger.info(
      `New consumer created with stream name ${params.stream}, consumer id ${consumerId} and offset ${params.offset.type}`,
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
    await consumer.close()
    this.consumers.delete(consumerId)
    this.logger.info(`Closed consumer with id: ${consumerId}`)
    return res.ok
  }

  public async declareSuperStreamConsumer(superStream: string, handle: ConsumerFunc): Promise<SuperStreamConsumer> {
    const consumerRef = `${superStream}-${randomUUID()}`
    const partitions = await this.queryPartitions({ superStream })
    return SuperStreamConsumer.create(handle, { locator: this, consumerRef, partitions })
  }

  private async closeAllConsumers() {
    await Promise.all([...this.consumers.values()].map((c) => c.close()))
    this.consumers = new Map<number, StreamConsumer>()
  }

  private async closeAllPublishers() {
    await Promise.all([...this.publishers.values()].map((c) => c.publisher.close()))
    this.publishers = new Map<number, { client: Client; publisher: StreamPublisher }>()
  }

  public consumerCounts() {
    return this.consumers.size
  }

  public publisherCounts() {
    return this.publishers.size
  }

  public get currentFrameMax() {
    return this.frameMax
  }

  public getConsumers() {
    return Array.from(this.consumers.values())
  }

  public send(cmd: Request): Promise<void> {
    return new Promise((res, rej) => {
      const bufferSizeParams = this.getBufferSizeParams()
      const body = cmd.toBuffer(bufferSizeParams)
      this.logger.debug(
        `Write cmd key: ${cmd.key.toString(16)} - no correlationId - data: ${inspect(body.toJSON())} length: ${
          body.byteLength
        }`,
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

  public async createSuperStream(
    params: {
      streamName: string
      arguments: CreateStreamArguments
    },
    bindingKeys?: string[],
    numberOfPartitions = 3,
  ): Promise<true> {
    if (lt(coerce(this.rabbitManagementVersion)!, REQUIRED_MANAGEMENT_VERSION)) {
      throw new Error(
        `Rabbitmq Management version ${this.rabbitManagementVersion} does not handle Create Super Stream Command. To create the stream use the cli`,
      )
    }

    this.logger.debug(`Create Super Stream...`)
    const { partitions, streamBindingKeys } = this.createSuperStreamPartitionsAndBindingKeys(
      params.streamName,
      numberOfPartitions,
      bindingKeys,
    )
    const res = await this.sendAndWait<CreateSuperStreamResponse>(
      new CreateSuperStreamRequest({ ...params, partitions, bindingKeys: streamBindingKeys }),
    )
    if (res.code === STREAM_ALREADY_EXISTS_ERROR_CODE) {
      return true
    }
    if (!res.ok) {
      throw new Error(`Create Super Stream command returned error with code ${res.code}`)
    }

    this.logger.debug(`Create Super Stream response: ${res.ok} - with arguments: '${inspect(params.arguments)}'`)
    return res.ok
  }

  public async deleteSuperStream(params: { streamName: string }): Promise<true> {
    if (lt(coerce(this.rabbitManagementVersion)!, REQUIRED_MANAGEMENT_VERSION)) {
      throw new Error(
        `Rabbitmq Management version ${this.rabbitManagementVersion} does not handle Delete Super Stream Command. To delete the stream use the cli`,
      )
    }

    this.logger.debug(`Delete Super Stream...`)
    const res = await this.sendAndWait<DeleteSuperStreamResponse>(new DeleteSuperStreamRequest(params.streamName))
    if (!res.ok) {
      throw new Error(`Delete Super Stream command returned error with code ${res.code}`)
    }
    this.logger.debug(`Delete Super Stream response: ${res.ok} - '${inspect(params.streamName)}'`)
    return res.ok
  }

  public async queryPublisherSequence(params: { stream: string; publisherRef: string }): Promise<bigint> {
    const res = await this.sendAndWait<QueryPublisherResponse>(new QueryPublisherRequest(params))
    if (!res.ok) {
      throw new Error(
        `Query Publisher Sequence command returned error with code ${res.code} - ${errorMessageOf(res.code)}`,
      )
    }

    this.logger.info(
      `Sequence for stream name ${params.stream}, publisher ref ${params.publisherRef} at ${res.sequence}`,
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

  public async queryOffset(params: QueryOffsetParams): Promise<bigint> {
    this.logger.debug(`Query Offset...`)
    const res = await this.sendAndWait<QueryOffsetResponse>(new QueryOffsetRequest(params))
    if (!res.ok) {
      throw new Error(`Query offset command returned error with code ${res.code}`)
    }
    this.logger.debug(`Query Offset response: ${res.ok} with params: '${inspect(params)}'`)
    return res.offsetValue
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

  private async exchangeCommandVersions() {
    const versions = getClientSupportedVersions(this.peerProperties.version)
    const response = await this.sendAndWait<ExchangeCommandVersionsResponse>(
      new ExchangeCommandVersionsRequest(versions),
    )
    this.serverDeclaredVersions.push(...response.serverDeclaredVersions)

    checkServerDeclaredVersions(this.serverVersions, this.logger, this.peerProperties.version)
    return response
  }

  public async subscribe(params: SubscribeParams): Promise<SubscribeResponse> {
    const res = await this.sendAndWait<SubscribeResponse>(new SubscribeRequest({ ...params }))
    if (!res.ok) {
      throw new Error(`Subscribe command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    return res
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

  public async routeQuery(params: { routingKey: string; superStream: string }) {
    const res = await this.sendAndWait<RouteResponse>(new RouteQuery(params))
    if (!res.ok) {
      throw new Error(`Route Query command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Route Response for super stream ${params.superStream}, ${res.streams}`)
    return res.streams
  }

  public async partitionsQuery(params: { superStream: string }) {
    const res = await this.sendAndWait<PartitionsResponse>(new PartitionsQuery(params))
    if (!res.ok) {
      throw new Error(`Partitions Query command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Partitions Response for super stream ${params.superStream}, ${res.streams}`)
    return res.streams
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
      new SaslAuthenticateRequest({ ...params, mechanism: "PLAIN" }),
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

  private sendAndWait<T extends Response>(cmd: Request): Promise<T> {
    return new Promise((res, rej) => {
      const correlationId = this.incCorrelationId()
      const bufferSizeParams = this.getBufferSizeParams()
      const body = cmd.toBuffer(bufferSizeParams, correlationId)
      this.logger.debug(
        `Write cmd key: ${cmd.key.toString(16)} - correlationId: ${correlationId}: data: ${inspect(
          body.toJSON(),
        )} length: ${body.byteLength}`,
      )
      this.socket.write(body, (err) => {
        this.logger.debug(
          `Write COMPLETED for cmd key: ${cmd.key.toString(16)} - correlationId: ${correlationId} err: ${err}`,
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
            16,
          )} found key: ${response.key.toString(16)} `,
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
      this.logger.debug(`on deliver -> ${consumer.consumerRef}`)
      this.logger.debug(`response.messages.length: ${response.messages.length}`)
      await this.askForCredit({ credit: 1, subscriptionId: response.subscriptionId })
      response.messages.map((x) => consumer.handle(x))
    })
  }

  private registerConsumerUpdateQuery() {
    this.decoder.on("consumer_update_query", async (response: ConsumerUpdateQuery) => {
      const consumer = this.consumers.get(response.subscriptionId)
      if (!consumer) {
        this.logger.error(`On consumer_update_query no consumer found`)
        return
      }
      this.logger.debug(`on consumer_update_query -> ${consumer.consumerRef}`)
      await this.send(
        new ConsumerUpdateResponse({ correlationId: response.correlationId, responseCode: 1, offset: consumer.offset }),
      )
    })
  }

  private calculateFrameMaxSizeFrom(tuneResponseFrameMax: number) {
    if (this.frameMax === DEFAULT_UNLIMITED_FRAME_MAX) return tuneResponseFrameMax
    if (tuneResponseFrameMax === DEFAULT_UNLIMITED_FRAME_MAX) return this.frameMax
    return Math.min(this.frameMax, tuneResponseFrameMax)
  }

  private getBufferSizeParams(): BufferSizeParams {
    return { maxSize: this.frameMax, ...this.bufferSizeSettings }
  }

  private async initNewClient(
    streamName: string,
    leader: boolean,
    connectionClosedListener?: ConnectionClosedListener,
  ): Promise<Client> {
    const [metadata] = await this.queryMetadata({ streams: [streamName] })
    const chosenNode = chooseNode(metadata, leader)
    if (!chosenNode) {
      throw new Error(`Stream was not found on any node`)
    }
    const cachedClient = Client.getUsableCachedClient(leader, streamName, chosenNode.host)
    if (cachedClient) {
      cachedClient.incrRefCount()
      return cachedClient
    }
    const listeners = { ...this.params.listeners, connection_closed: connectionClosedListener }
    const connectionParams = { ...this.params, listeners: listeners, leader: leader, streamName: streamName }
    const newClient = await this.getConnectionOnChosenNode(chosenNode, connectionParams, metadata)
    newClient.incrRefCount()
    Client.cacheClient(leader, streamName, chosenNode.host, newClient)
    return newClient
  }

  private async getConnectionOnChosenNode(
    chosenNode: { host: string; port: number },
    connectionParams: ConnectionParams,
    metadata: StreamMetadata,
  ): Promise<Client> {
    if (this.params.addressResolver && this.params.addressResolver.enabled) {
      const maxAttempts = computeMaxAttempts(metadata)
      const resolver = this.params.addressResolver
      let currentAttempt = 0
      while (currentAttempt < maxAttempts) {
        this.logger.debug(`Attempting to connect using the address resolver - attempt ${currentAttempt + 1}`)
        const hostname = resolver.endpoint?.host ?? this.params.hostname
        const port = resolver.endpoint?.port ?? this.params.port
        const client = await connect({ ...connectionParams, hostname, port }, this.logger)
        if (client.serverEndpoint.host === chosenNode.host && client.serverEndpoint.port === chosenNode.port) {
          this.logger.debug(`Correct connection was found!`)
          return client
        }
        this.logger.debug(`The node found was not the right one - closing the connection`)
        await client.close()
        currentAttempt++
      }
      throw new Error(`Could not find broker (${chosenNode.host}:${chosenNode.port}) after ${maxAttempts} attempts`)
    }
    return connect({ ...connectionParams, hostname: chosenNode.host, port: chosenNode.port }, this.logger)
  }

  private createSuperStreamPartitionsAndBindingKeys(
    streamName: string,
    numberOfPartitions: number,
    bindingKeys?: string[],
  ) {
    const partitions: string[] = []
    if (!bindingKeys) {
      for (let i = 0; i < numberOfPartitions; i++) {
        partitions.push(`${streamName}-${i}`)
      }
      const streamBindingKeys = Array.from(Array(numberOfPartitions).keys()).map((n) => `${n}`)
      return { partitions, streamBindingKeys }
    }
    bindingKeys.map((bk) => partitions.push(`${streamName}-${bk}`))
    return { partitions, streamBindingKeys: bindingKeys }
  }
  private incrRefCount() {
    ++this.refCount
  }

  private decrRefCount() {
    return --this.refCount
  }

  private static getUsableCachedClient(leader: boolean, streamName: string, host: string) {
    const m = leader ? Client.publisherClients : Client.consumerClients
    const k = Client.getCacheKey(streamName, host)
    const clients = m.get(k) || []
    const client = clients.at(-1)
    const refCount = client?.refCount
    return refCount && refCount < getMaxSharedClientInstances() ? client : undefined
  }

  private static cacheClient(leader: boolean, streamName: string, host: string, client: Client) {
    const m = leader ? Client.publisherClients : Client.consumerClients
    const k = Client.getCacheKey(streamName, host)
    const currentlyCached = m.get(k) || []
    currentlyCached.push(client)
    m.set(k, currentlyCached)
  }

  private static removeCachedClient(client: Client) {
    const leader = client.leader
    const streamName = client.streamName
    const host = client.hostname
    if (streamName === undefined) return
    const m = leader ? Client.publisherClients : Client.consumerClients
    const k = Client.getCacheKey(streamName, host)
    const mappedClientList = m.get(k)
    if (mappedClientList) {
      const filtered = mappedClientList.filter((c) => c !== client)
      m.set(k, filtered)
    }
  }

  private static getCacheKey(streamName: string, host: string) {
    return `${streamName}@${host}`
  }
}

export type ListenersParams = {
  metadata_update?: MetadataUpdateListener
  publish_confirm?: PublishConfirmListener
  publish_error?: PublishErrorListener
  connection_closed?: ConnectionClosedListener
}

export interface SSLConnectionParams {
  key: string
  cert: string
  ca?: string
}

export type AddressResolverParams =
  | {
      enabled: true
      endpoint?: { host: string; port: number }
    }
  | { enabled: false }

export interface ConnectionParams {
  hostname: string
  port: number
  username: string
  password: string
  vhost: string
  frameMax?: number
  heartbeat?: number
  listeners?: ListenersParams
  ssl?: SSLConnectionParams
  bufferSizeSettings?: BufferSizeSettings
  socketTimeout?: number
  addressResolver?: AddressResolverParams
  leader?: boolean
  streamName?: string
}

export interface DeclarePublisherParams {
  stream: string
  publisherRef?: string
  boot?: boolean
  maxChunkLength?: number
  connectionClosedListener?: ConnectionClosedListener
}

export interface DeclareConsumerParams {
  stream: string
  consumerRef?: string
  offset: Offset
  connectionClosedListener?: ConnectionClosedListener
  singleActive?: boolean
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

export interface QueryOffsetParams {
  reference: string
  stream: string
}

export interface QueryMetadataParams {
  streams: string[]
}

export interface QueryPartitionsParams {
  superStream: string
}

export function connect(params: ConnectionParams, logger?: Logger): Promise<Client> {
  return Client.connect(params, logger)
}

function errorMessageOf(code: number): string {
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

function extractHeartbeatInterval(heartbeatInterval: number, tuneResponse: TuneResponse): number {
  return heartbeatInterval === 0 ? tuneResponse.heartbeat : Math.min(heartbeatInterval, tuneResponse.heartbeat)
}

const addOffsetFilterToHandle = (handle: ConsumerFunc, offset: Offset): ConsumerFunc => {
  if (offset.type === "numeric") {
    const handlerWithFilter = (message: Message) => {
      if (message.offset !== undefined && message.offset < offset.value!) {
        return
      }
      handle(message)
    }
    return handlerWithFilter
  }
  return handle
}

const chooseNode = (metadata: { leader?: Broker; replicas?: Broker[] }, leader: boolean): Broker | undefined => {
  if (leader) {
    return metadata.leader
  }
  const chosenNode = metadata.replicas?.length ? sample(metadata.replicas) : metadata.leader
  return chosenNode
}

const computeMaxAttempts = (metadata: { leader?: Broker; replicas?: Broker[] }): number => {
  return Math.pow(2 + (metadata.leader ? 1 : 0) + (metadata.replicas?.length ?? 0), 2)
}
