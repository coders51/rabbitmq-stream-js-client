import { randomUUID } from "crypto"
import { inspect } from "util"
import { Compression, CompressionType, GzipCompression, NoneCompression } from "./compression"
import { Consumer, ConsumerFunc, StreamConsumer } from "./consumer"
import { STREAM_ALREADY_EXISTS_ERROR_CODE } from "./error_codes"
import { Logger, NullLogger } from "./logger"
import { FilterFunc, Message, Publisher, StreamPublisher } from "./publisher"
import { ConsumerUpdateResponse } from "./requests/consumer_update_response"
import { CreateStreamArguments, CreateStreamRequest } from "./requests/create_stream_request"
import { CreditRequest, CreditRequestParams } from "./requests/credit_request"
import { DeclarePublisherRequest } from "./requests/declare_publisher_request"
import { DeletePublisherRequest } from "./requests/delete_publisher_request"
import { DeleteStreamRequest } from "./requests/delete_stream_request"
import { MetadataRequest } from "./requests/metadata_request"
import { PartitionsQuery } from "./requests/partitions_query"
import { BufferSizeSettings, Request } from "./requests/request"
import { RouteQuery } from "./requests/route_query"
import { StreamStatsRequest } from "./requests/stream_stats_request"
import { Offset, SubscribeRequest } from "./requests/subscribe_request"
import { UnsubscribeRequest } from "./requests/unsubscribe_request"
import { MetadataUpdateListener, PublishConfirmListener, PublishErrorListener } from "./response_decoder"
import { ConsumerUpdateQuery } from "./responses/consumer_update_query"
import { CreateStreamResponse } from "./responses/create_stream_response"
import { DeclarePublisherResponse } from "./responses/declare_publisher_response"
import { DeletePublisherResponse } from "./responses/delete_publisher_response"
import { DeleteStreamResponse } from "./responses/delete_stream_response"
import { DeliverResponse } from "./responses/deliver_response"
import { Broker, MetadataResponse, StreamMetadata } from "./responses/metadata_response"
import { PartitionsResponse } from "./responses/partitions_response"
import { RouteResponse } from "./responses/route_response"
import { StreamStatsResponse } from "./responses/stream_stats_response"
import { SubscribeResponse } from "./responses/subscribe_response"
import { UnsubscribeResponse } from "./responses/unsubscribe_response"
import { SuperStreamConsumer } from "./super_stream_consumer"
import { MessageKeyExtractorFunction, SuperStreamPublisher } from "./super_stream_publisher"
import { DEFAULT_FRAME_MAX, REQUIRED_MANAGEMENT_VERSION, sample } from "./util"
import { CreateSuperStreamRequest } from "./requests/create_super_stream_request"
import { CreateSuperStreamResponse } from "./responses/create_super_stream_response"
import { DeleteSuperStreamResponse } from "./responses/delete_super_stream_response"
import { DeleteSuperStreamRequest } from "./requests/delete_super_stream_request"
import { lt, coerce } from "semver"
import { ConnectionInfo, Connection, errorMessageOf } from "./connection"
import { ConnectionPool } from "./connection_pool"
import { DeliverResponseV2 } from "./responses/deliver_response_v2"

export type ConnectionClosedListener = (hadError: boolean) => void

export type ClosingParams = { closingCode: number; closingReason: string }

export class Client {
  private id: string = randomUUID()
  private publisherId = 0
  private consumerId = 0
  private consumers = new Map<number, StreamConsumer>()
  private publishers = new Map<number, { connection: Connection; publisher: StreamPublisher }>()
  private compressions = new Map<CompressionType, Compression>()
  private readonly connection: Connection

  private constructor(private readonly logger: Logger, private readonly params: ClientParams, connection?: Connection) {
    this.compressions.set(CompressionType.None, NoneCompression.create())
    this.compressions.set(CompressionType.Gzip, GzipCompression.create())
    this.connection = connection ?? this.getLocatorConnection()
    this.connection.incrRefCount()
  }

  getCompression(compressionType: CompressionType) {
    return this.connection.getCompression(compressionType)
  }

  registerCompression(compression: Compression) {
    this.connection.registerCompression(compression)
  }

  public start(): Promise<Client> {
    return this.connection.start().then(
      (_res) => {
        return this
      },
      (rej) => {
        if (rej instanceof Error) throw rej
        throw new Error(`${inspect(rej)}`)
      }
    )
  }

  public async close(params: ClosingParams = { closingCode: 0, closingReason: "" }) {
    this.logger.info(`${this.id} Closing client...`)
    if (this.publisherCounts()) {
      this.logger.info(`Stopping all producers...`)
      await this.closeAllPublishers()
    }
    if (this.consumerCounts()) {
      this.logger.info(`Stopping all consumers...`)
      await this.closeAllConsumers()
    }
    this.connection.decrRefCount()
    await this.closeConnectionIfUnused(this.connection, params)
  }

  private async closeConnectionIfUnused(connection: Connection, params: ClosingParams) {
    if (connection.refCount <= 0) {
      ConnectionPool.removeCachedConnection(this.connection)
      await this.connection.close(params)
    }
  }

  public async queryMetadata(params: QueryMetadataParams): Promise<StreamMetadata[]> {
    const { streams } = params
    const res = await this.connection.sendAndWait<MetadataResponse>(new MetadataRequest({ streams }))
    if (!res.ok) {
      throw new Error(`Query Metadata command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Returned stream metadata for streams with names ${params.streams.join(",")}`)
    const { streamInfos } = res

    return streamInfos
  }

  public async queryPartitions(params: QueryPartitionsParams): Promise<string[]> {
    const { superStream } = params
    const res = await this.connection.sendAndWait<PartitionsResponse>(new PartitionsQuery({ superStream }))
    if (!res.ok) {
      throw new Error(`Query Partitions command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Returned superstream partitions for superstream ${superStream}`)
    return res.streams
  }

  public async declarePublisher(params: DeclarePublisherParams, filter?: FilterFunc): Promise<Publisher> {
    const { stream, publisherRef } = params
    const publisherId = this.incPublisherId()

    const connection = await this.getConnection(params.stream, true, params.connectionClosedListener)
    const res = await connection.sendAndWait<DeclarePublisherResponse>(
      new DeclarePublisherRequest({ stream, publisherRef, publisherId })
    )
    if (!res.ok) {
      await connection.close()
      throw new Error(`Declare Publisher command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    if (filter && !connection.isFilteringEnabled) {
      throw new Error(`Broker does not support message filtering.`)
    }
    const publisher = new StreamPublisher(
      {
        connection: connection,
        stream: params.stream,
        publisherId: publisherId,
        publisherRef: params.publisherRef,
        boot: params.boot,
        maxFrameSize: this.maxFrameSize,
        maxChunkLength: params.maxChunkLength,
        logger: this.logger,
      },
      filter
    )
    this.publishers.set(publisherId, { publisher: publisher, connection: connection })
    this.logger.info(
      `New publisher created with stream name ${params.stream}, publisher id ${publisherId} and publisher reference ${params.publisherRef}`
    )

    return publisher
  }

  public async deletePublisher(publisherId: number) {
    const publisherConnection = this.publishers.get(publisherId)?.connection ?? this.connection
    const res = await publisherConnection.sendAndWait<DeletePublisherResponse>(new DeletePublisherRequest(publisherId))
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
    const connection = await this.getConnection(params.stream, false, params.connectionClosedListener)

    if (params.filter && !connection.isFilteringEnabled) {
      throw new Error(`Broker does not support message filtering.`)
    }

    const consumer = new StreamConsumer(
      addOffsetFilterToHandle(handle, params.offset),
      {
        connection,
        stream: params.stream,
        consumerId,
        consumerRef: params.consumerRef,
        offset: params.offset,
      },
      params.filter
    )
    this.consumers.set(consumerId, consumer)

    if (params.singleActive && !params.consumerRef) {
      throw new Error("consumerRef is mandatory when declaring a single active consumer")
    }
    if (params.singleActive) {
      properties["single-active-consumer"] = "true"
      properties["name"] = params.consumerRef!
    }
    if (params.filter) {
      for (let i = 0; i < params.filter.values.length; i++) {
        properties[`filter.${i}`] = params.filter.values[i]
      }
      properties["match-unfiltered"] = `${params.filter.matchUnfiltered}`
    }

    const res = await this.connection.sendAndWait<SubscribeResponse>(
      new SubscribeRequest({ ...params, subscriptionId: consumerId, credit: 10, properties: properties })
    )

    if (!res.ok) {
      this.consumers.delete(consumerId)
      throw new Error(`Declare Consumer command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }

    this.logger.info(
      `New consumer created with stream name ${params.stream}, consumer id ${consumerId} and offset ${params.offset.type}`
    )
    return consumer
  }

  public async closeConsumer(consumerId: number) {
    const consumer = this.consumers.get(consumerId)
    if (!consumer) {
      this.logger.error("Consumer does not exist")
      throw new Error(`Consumer with id: ${consumerId} does not exist`)
    }
    const res = await this.connection.sendAndWait<UnsubscribeResponse>(new UnsubscribeRequest(consumerId))
    if (!res.ok) {
      throw new Error(`Unsubscribe command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    await consumer.close()
    this.consumers.delete(consumerId)
    this.logger.info(`Closed consumer with id: ${consumerId}`)
    return res.ok
  }

  public async declareSuperStreamConsumer(
    { superStream }: DeclareSuperStreamConsumerParams,
    handle: ConsumerFunc
  ): Promise<SuperStreamConsumer> {
    const consumerRef = `${superStream}-${randomUUID()}`
    const partitions = await this.queryPartitions({ superStream })
    return SuperStreamConsumer.create(handle, { locator: this, consumerRef, partitions })
  }

  public async declareSuperStreamPublisher(
    { superStream, publisherRef, routingStrategy }: DeclareSuperStreamPublisherParams,
    keyExtractor: MessageKeyExtractorFunction
  ): Promise<SuperStreamPublisher> {
    return SuperStreamPublisher.create({
      locator: this,
      superStream: superStream,
      keyExtractor,
      publisherRef,
      routingStrategy,
    })
  }

  private async closeAllConsumers() {
    await Promise.all([...this.consumers.values()].map((c) => c.close()))
    this.consumers = new Map<number, StreamConsumer>()
  }

  private async closeAllPublishers() {
    await Promise.all([...this.publishers.values()].map((c) => c.publisher.close()))
    this.publishers = new Map<number, { connection: Connection; publisher: StreamPublisher }>()
  }

  public consumerCounts() {
    return this.consumers.size
  }

  public publisherCounts() {
    return this.publishers.size
  }

  public getConsumers() {
    return Array.from(this.consumers.values())
  }

  public send(cmd: Request): Promise<void> {
    return this.connection.send(cmd)
  }

  public async createStream(params: { stream: string; arguments?: CreateStreamArguments }): Promise<true> {
    this.logger.debug(`Create Stream...`)
    const res = await this.connection.sendAndWait<CreateStreamResponse>(new CreateStreamRequest(params))
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
    const res = await this.connection.sendAndWait<DeleteStreamResponse>(new DeleteStreamRequest(params.stream))
    if (!res.ok) {
      throw new Error(`Delete Stream command returned error with code ${res.code}`)
    }
    this.logger.debug(`Delete Stream response: ${res.ok} - '${inspect(params.stream)}'`)
    return res.ok
  }

  public async createSuperStream(
    params: {
      streamName: string
      arguments?: CreateStreamArguments
    },
    bindingKeys?: string[],
    numberOfPartitions = 3
  ): Promise<true> {
    if (lt(coerce(this.rabbitManagementVersion)!, REQUIRED_MANAGEMENT_VERSION)) {
      throw new Error(
        `Rabbitmq Management version ${this.rabbitManagementVersion} does not handle Create Super Stream Command. To create the stream use the cli`
      )
    }

    this.logger.debug(`Create Super Stream...`)
    const { partitions, streamBindingKeys } = this.createSuperStreamPartitionsAndBindingKeys(
      params.streamName,
      numberOfPartitions,
      bindingKeys
    )
    const res = await this.connection.sendAndWait<CreateSuperStreamResponse>(
      new CreateSuperStreamRequest({ ...params, partitions, bindingKeys: streamBindingKeys })
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
        `Rabbitmq Management version ${this.rabbitManagementVersion} does not handle Delete Super Stream Command. To delete the stream use the cli`
      )
    }

    this.logger.debug(`Delete Super Stream...`)
    const res = await this.connection.sendAndWait<DeleteSuperStreamResponse>(
      new DeleteSuperStreamRequest(params.streamName)
    )
    if (!res.ok) {
      throw new Error(`Delete Super Stream command returned error with code ${res.code}`)
    }
    this.logger.debug(`Delete Super Stream response: ${res.ok} - '${inspect(params.streamName)}'`)
    return res.ok
  }

  public async streamStatsRequest(streamName: string) {
    const res = await this.connection.sendAndWait<StreamStatsResponse>(new StreamStatsRequest(streamName))
    if (!res.ok) {
      throw new Error(`Stream Stats command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Statistics for stream name ${streamName}, ${res.statistics}`)
    return res.statistics
  }

  public getConnectionInfo(): ConnectionInfo {
    return this.connection.getConnectionInfo()
  }

  public async subscribe(params: SubscribeParams): Promise<SubscribeResponse> {
    const res = await this.connection.sendAndWait<SubscribeResponse>(new SubscribeRequest({ ...params }))
    if (!res.ok) {
      throw new Error(`Subscribe command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    return res
  }

  public get maxFrameSize() {
    return this.connection.maxFrameSize ?? DEFAULT_FRAME_MAX
  }

  public get serverVersions() {
    return this.connection.serverVersions
  }

  public get rabbitManagementVersion() {
    return this.connection.rabbitManagementVersion
  }

  public async routeQuery(params: { routingKey: string; superStream: string }) {
    const res = await this.connection.sendAndWait<RouteResponse>(new RouteQuery(params))
    if (!res.ok) {
      throw new Error(`Route Query command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Route Response for super stream ${params.superStream}, ${res.streams}`)
    return res.streams
  }

  public async partitionsQuery(params: { superStream: string }) {
    const res = await this.connection.sendAndWait<PartitionsResponse>(new PartitionsQuery(params))
    if (!res.ok) {
      throw new Error(`Partitions Query command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Partitions Response for super stream ${params.superStream}, ${res.streams}`)
    return res.streams
  }

  private askForCredit(params: CreditRequestParams): Promise<void> {
    return this.send(new CreditRequest({ ...params }))
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

  private getDeliverV1Callback() {
    return async (response: DeliverResponse) => {
      const consumer = this.consumers.get(response.subscriptionId)
      if (!consumer) {
        this.logger.error(`On deliverV1 no consumer found`)
        return
      }
      this.logger.debug(`on deliverV1 -> ${consumer.consumerRef}`)
      this.logger.debug(`response.messages.length: ${response.messages.length}`)
      await this.askForCredit({ credit: 1, subscriptionId: response.subscriptionId })
      response.messages.map((x) => consumer.handle(x))
    }
  }

  private getDeliverV2Callback() {
    return async (response: DeliverResponseV2) => {
      const consumer = this.consumers.get(response.subscriptionId)
      if (!consumer) {
        this.logger.error(`On deliverV2 no consumer found`)
        return
      }
      this.logger.debug(`on deliverV2 -> ${consumer.consumerRef}`)
      this.logger.debug(`response.messages.length: ${response.messages.length}`)
      await this.askForCredit({ credit: 1, subscriptionId: response.subscriptionId })
      if (consumer.filter) {
        response.messages.filter((x) => consumer.filter?.postFilterFunc(x)).map((x) => consumer.handle(x))
        return
      }
      response.messages.map((x) => consumer.handle(x))
    }
  }

  private getConsumerUpdateCallback() {
    return async (response: ConsumerUpdateQuery) => {
      const consumer = this.consumers.get(response.subscriptionId)
      if (!consumer) {
        this.logger.error(`On consumer_update_query no consumer found`)
        return
      }
      this.logger.debug(`on consumer_update_query -> ${consumer.consumerRef}`)
      await this.send(
        new ConsumerUpdateResponse({ correlationId: response.correlationId, responseCode: 1, offset: consumer.offset })
      )
    }
  }

  private getLocatorConnection() {
    const connectionParams = this.buildConnectionParams(false, "", this.params.listeners?.connection_closed)
    return Connection.create(connectionParams, this.logger)
  }

  private async getConnection(
    streamName: string,
    leader: boolean,
    connectionClosedListener?: ConnectionClosedListener
  ): Promise<Connection> {
    const [metadata] = await this.queryMetadata({ streams: [streamName] })
    const chosenNode = chooseNode(metadata, leader)
    if (!chosenNode) {
      throw new Error(`Stream was not found on any node`)
    }
    const cachedConnection = ConnectionPool.getUsableCachedConnection(leader, streamName, chosenNode.host)
    if (cachedConnection) return cachedConnection

    const newConnection = await this.getConnectionOnChosenNode(
      leader,
      streamName,
      chosenNode,
      metadata,
      connectionClosedListener
    )

    ConnectionPool.cacheConnection(leader, streamName, newConnection.hostname, newConnection)
    return newConnection
  }

  private createSuperStreamPartitionsAndBindingKeys(
    streamName: string,
    numberOfPartitions: number,
    bindingKeys?: string[]
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

  private buildConnectionParams(
    leader: boolean,
    streamName: string,
    connectionClosedListener?: ConnectionClosedListener
  ) {
    const connectionListeners = {
      ...this.params.listeners,
      connection_closed: connectionClosedListener,
      deliverV1: this.getDeliverV1Callback(),
      deliverV2: this.getDeliverV2Callback(),
      consumer_update_query: this.getConsumerUpdateCallback(),
    }
    return { ...this.params, listeners: connectionListeners, leader: leader, streamName: streamName }
  }

  private async getConnectionOnChosenNode(
    leader: boolean,
    streamName: string,
    chosenNode: { host: string; port: number },
    metadata: StreamMetadata,
    connectionClosedListener?: ConnectionClosedListener
  ): Promise<Connection> {
    const connectionParams = this.buildConnectionParams(leader, streamName, connectionClosedListener)
    if (this.params.addressResolver && this.params.addressResolver.enabled) {
      const maxAttempts = computeMaxAttempts(metadata)
      const resolver = this.params.addressResolver
      let currentAttempt = 0
      while (currentAttempt < maxAttempts) {
        this.logger.debug(`Attempting to connect using the address resolver - attempt ${currentAttempt + 1}`)
        const hostname = resolver.endpoint?.host ?? this.params.hostname
        const port = resolver.endpoint?.port ?? this.params.port
        const connection = await Connection.connect({ ...connectionParams, hostname, port }, this.logger)
        const { host: connectionHost, port: connectionPort } = connection.getConnectionInfo()
        if (connectionHost === chosenNode.host && connectionPort === chosenNode.port) {
          this.logger.debug(`Correct connection was found!`)
          return connection
        }
        this.logger.debug(`The node found was not the right one - closing the connection`)
        await connection.close()
        currentAttempt++
      }
      throw new Error(`Could not find broker (${chosenNode.host}:${chosenNode.port}) after ${maxAttempts} attempts`)
    }
    return Connection.connect({ ...connectionParams, hostname: chosenNode.host, port: chosenNode.port }, this.logger)
  }

  static async connect(params: ClientParams, logger?: Logger): Promise<Client> {
    return new Client(logger ?? new NullLogger(), params).start()
  }
}

export type ClientListenersParams = {
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

export interface ClientParams {
  hostname: string
  port: number
  username: string
  password: string
  vhost: string
  frameMax?: number
  heartbeat?: number
  listeners?: ClientListenersParams
  ssl?: SSLConnectionParams
  bufferSizeSettings?: BufferSizeSettings
  socketTimeout?: number
  addressResolver?: AddressResolverParams
  leader?: boolean
  streamName?: string
  connectionName?: string
}

export interface DeclarePublisherParams {
  stream: string
  publisherRef?: string
  boot?: boolean
  maxChunkLength?: number
  connectionClosedListener?: ConnectionClosedListener
}

export type RoutingStrategy = "key" | "hash"

export interface DeclareSuperStreamPublisherParams {
  superStream: string
  publisherRef?: string
  routingStrategy?: RoutingStrategy
}

export interface ConsumerFilter {
  values: string[]
  postFilterFunc: (msg: Message) => boolean
  matchUnfiltered: boolean
}

export interface DeclareConsumerParams {
  stream: string
  consumerRef?: string
  offset: Offset
  connectionClosedListener?: ConnectionClosedListener
  singleActive?: boolean
  filter?: ConsumerFilter
}

export interface DeclareSuperStreamConsumerParams {
  superStream: string
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

export function connect(params: ClientParams, logger?: Logger): Promise<Client> {
  return Client.connect(params, logger)
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
