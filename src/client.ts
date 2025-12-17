import { randomUUID } from "crypto"
import { coerce, lt } from "semver"
import { inspect } from "util"
import { Compression, CompressionType, GzipCompression, NoneCompression } from "./compression"
import { Connection, ConnectionInfo, ConnectionParams, errorMessageOf } from "./connection"
import { ConnectionPool, ConnectionPurpose } from "./connection_pool"
import { Consumer, ConsumerFunc, ConsumerUpdateListener, StreamConsumer, computeExtendedConsumerId } from "./consumer"
import { STREAM_ALREADY_EXISTS_ERROR_CODE } from "./error_codes"
import { Logger, NullLogger } from "./logger"
import { FilterFunc, Message, Publisher, StreamPublisher } from "./publisher"
import { ConsumerUpdateResponse } from "./requests/consumer_update_response"
import { CreateStreamArguments, CreateStreamRequest } from "./requests/create_stream_request"
import { CreateSuperStreamRequest } from "./requests/create_super_stream_request"
import { CreditRequest } from "./requests/credit_request"
import { DeclarePublisherRequest } from "./requests/declare_publisher_request"
import { DeletePublisherRequest } from "./requests/delete_publisher_request"
import { DeleteStreamRequest } from "./requests/delete_stream_request"
import { DeleteSuperStreamRequest } from "./requests/delete_super_stream_request"
import { MetadataRequest } from "./requests/metadata_request"
import { PartitionsQuery } from "./requests/partitions_query"
import { BufferSizeSettings } from "./requests/request"
import { RouteQuery } from "./requests/route_query"
import { StreamStatsRequest } from "./requests/stream_stats_request"
import { Offset, SubscribeRequest } from "./requests/subscribe_request"
import { UnsubscribeRequest } from "./requests/unsubscribe_request"
import { MetadataUpdateListener } from "./response_decoder"
import { ConsumerUpdateQuery } from "./responses/consumer_update_query"
import { CreateStreamResponse } from "./responses/create_stream_response"
import { CreateSuperStreamResponse } from "./responses/create_super_stream_response"
import { DeclarePublisherResponse } from "./responses/declare_publisher_response"
import { DeletePublisherResponse } from "./responses/delete_publisher_response"
import { DeleteStreamResponse } from "./responses/delete_stream_response"
import { DeleteSuperStreamResponse } from "./responses/delete_super_stream_response"
import { DeliverResponse } from "./responses/deliver_response"
import { DeliverResponseV2 } from "./responses/deliver_response_v2"
import { Broker, MetadataResponse, StreamMetadata } from "./responses/metadata_response"
import { PartitionsResponse } from "./responses/partitions_response"
import { RouteResponse } from "./responses/route_response"
import { StreamStatsResponse } from "./responses/stream_stats_response"
import { SubscribeResponse } from "./responses/subscribe_response"
import { UnsubscribeResponse } from "./responses/unsubscribe_response"
import { SuperStreamConsumer, SuperStreamConsumerFunc } from "./super_stream_consumer"
import { MessageKeyExtractorFunction, SuperStreamPublisher } from "./super_stream_publisher"
import { DEFAULT_FRAME_MAX, REQUIRED_MANAGEMENT_VERSION, ResponseCode, sample, wait } from "./util"
import { ConsumerCreditPolicy, CreditRequestWrapper, defaultCreditPolicy } from "./consumer_credit_policy"
import { PublishConfirmResponse } from "./responses/publish_confirm_response"
import { PublishErrorResponse } from "./responses/publish_error_response"

/**
 * Callback invoked when a connection is closed
 * @param hadError - True if the connection closed due to an error
 */
export type ConnectionClosedListener = (hadError: boolean) => void

/**
 * Callback invoked when a publish is confirmed by the server
 * @param confirm - The publish confirmation response
 * @param connectionId - The ID of the connection that sent the confirmation
 */
export type ConnectionPublishConfirmListener = (confirm: PublishConfirmResponse, connectionId: string) => void

/**
 * Callback invoked when a publish error occurs
 * @param confirm - The publish error response
 * @param connectionId - The ID of the connection that reported the error
 */
export type ConnectionPublishErrorListener = (confirm: PublishErrorResponse, connectionId: string) => void

/**
 * Parameters for closing connections
 */
export type ClosingParams = { closingCode: number; closingReason: string; manuallyClose?: boolean }

type ConsumerMappedValue = { connection: Connection; consumer: StreamConsumer; params: DeclareConsumerParams }
type PublisherMappedValue = {
  connection: Connection
  publisher: StreamPublisher
  params: DeclarePublisherParams
  filter: FilterFunc | undefined
}

type DeliverData = {
  messages: Message[]
  messageFilteringSupported: boolean
  subscriptionId: number
  consumerId: string
}

/**
 * Main RabbitMQ Stream client for managing connections, publishers, and consumers.
 *
 * The Client class serves as the primary entry point for interacting with RabbitMQ streams.
 * It manages:
 * - A locator connection for metadata queries and topology management
 * - A connection pool for sharing TCP connections among publishers and consumers
 * - Publisher and consumer lifecycle
 * - Stream creation and deletion
 *
 * @example
 * ```typescript
 * const client = await connect({
 *   hostname: 'localhost',
 *   port: 5552,
 *   username: 'guest',
 *   password: 'guest',
 *   vhost: '/'
 * });
 *
 * await client.createStream({ stream: 'my-stream' });
 * const publisher = await client.declarePublisher({ stream: 'my-stream' });
 * await publisher.send(Buffer.from('Hello World'));
 * await client.close();
 * ```
 */
export class Client {
  public readonly id: string = randomUUID()
  private consumers = new Map<string, ConsumerMappedValue>()
  private publishers = new Map<string, PublisherMappedValue>()
  private compressions = new Map<CompressionType, Compression>()
  private locatorConnection: Connection
  private pool: ConnectionPool

  private constructor(
    private readonly logger: Logger,
    private readonly params: ClientParams
  ) {
    this.compressions.set(CompressionType.None, NoneCompression.create())
    this.compressions.set(CompressionType.Gzip, GzipCompression.create())
    this.locatorConnection = this.getLocatorConnection()
    this.pool = new ConnectionPool(logger)
  }

  getCompression(compressionType: CompressionType) {
    return this.locatorConnection.getCompression(compressionType)
  }

  registerCompression(compression: Compression) {
    this.locatorConnection.registerCompression(compression)
  }

  /**
   * Start the client by establishing the locator connection
   * @returns A promise that resolves to the client instance
   */
  public start(): Promise<Client> {
    return this.locatorConnection.start().then(
      (_res) => {
        return this
      },
      (rej) => {
        if (rej instanceof Error) throw rej
        throw new Error(`${inspect(rej)}`)
      }
    )
  }

  /**
   * Close the client and all associated publishers, consumers, and connections
   * @param params - Optional closing parameters with code and reason
   * @returns A promise that resolves when the client is fully closed
   * @example
   * ```typescript
   * await client.close();
   * // or with custom parameters
   * await client.close({ closingCode: 1, closingReason: 'Shutting down' });
   * ```
   */
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
    await this.locatorConnection.close({ ...params, manuallyClose: true })
  }

  /**
   * Query metadata for one or more streams
   * @param params - Parameters containing the list of stream names to query
   * @returns A promise that resolves to an array of stream metadata
   * @throws {Error} If the query returns an error code
   */
  public async queryMetadata(params: QueryMetadataParams): Promise<StreamMetadata[]> {
    const { streams } = params
    const res = await this.locatorConnection.sendAndWait<MetadataResponse>(new MetadataRequest({ streams }))
    if (!res.ok) {
      throw new Error(`Query Metadata command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Returned stream metadata for streams with names ${params.streams.join(",")}`)
    const { streamInfos } = res

    return streamInfos
  }

  /**
   * Query the partitions of a super stream
   * @param params - Parameters containing the super stream name
   * @returns A promise that resolves to an array of partition stream names
   * @throws {Error} If the query returns an error code
   */
  public async queryPartitions(params: QueryPartitionsParams): Promise<string[]> {
    const { superStream } = params
    const res = await this.locatorConnection.sendAndWait<PartitionsResponse>(new PartitionsQuery({ superStream }))
    if (!res.ok) {
      throw new Error(`Query Partitions command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Returned superstream partitions for superstream ${superStream}`)
    return res.streams
  }

  /**
   * Declare a publisher for a stream
   *
   * Publishers are used to send messages to a stream. If a publisherRef is provided,
   * deduplication is enabled and the publisher will use monotonically increasing publishing IDs.
   *
   * @param params - Publisher configuration including stream name and optional publisherRef
   * @param filter - Optional filter function for server-side message filtering
   * @returns A promise that resolves to a Publisher instance
   * @throws {Error} If the declare command fails or filtering is not supported by the broker
   * @example
   * ```typescript
   * // Simple publisher
   * const publisher = await client.declarePublisher({ stream: 'my-stream' });
   * await publisher.send(Buffer.from('Hello'));
   *
   * // Publisher with deduplication
   * const dedupPublisher = await client.declarePublisher({
   *   stream: 'my-stream',
   *   publisherRef: 'unique-publisher-ref'
   * });
   * ```
   */
  public async declarePublisher(params: DeclarePublisherParams, filter?: FilterFunc): Promise<Publisher> {
    const connection = await this.getConnection(params.stream, "publisher", params.connectionClosedListener)
    const publisherId = connection.getNextPublisherId()
    await this.declarePublisherOnConnection(params, publisherId, connection, filter)
    const streamPublisherParams = {
      connection: connection,
      stream: params.stream,
      publisherId: publisherId,
      publisherRef: params.publisherRef,
      maxFrameSize: this.maxFrameSize,
      maxChunkLength: params.maxChunkLength,
      logger: this.logger,
    }
    let lastPublishingId = 0n
    if (streamPublisherParams.publisherRef) {
      lastPublishingId = await this.locatorConnection.queryPublisherSequence({
        stream: streamPublisherParams.stream,
        publisherRef: streamPublisherParams.publisherRef,
      })
    }
    const publisher = new StreamPublisher(this.pool, streamPublisherParams, lastPublishingId, filter)
    connection.registerForClosePublisher(publisher.extendedId, params.stream, async () => {
      await publisher.automaticClose()
      this.publishers.delete(publisher.extendedId)
    })
    this.publishers.set(publisher.extendedId, { publisher, connection, params, filter })
    this.logger.info(
      `New publisher created with stream name ${params.stream}, publisher id ${publisherId} and publisher reference ${params.publisherRef}`
    )
    return publisher
  }

  public async deletePublisher(extendedPublisherId: string) {
    const { publisher, connection } = this.publishers.get(extendedPublisherId) ?? {
      publisher: undefined,
      connection: this.locatorConnection,
    }
    const publisherId = extractPublisherId(extendedPublisherId)
    const res = await connection.sendAndWait<DeletePublisherResponse>(new DeletePublisherRequest(publisherId))
    if (!res.ok) {
      throw new Error(`Delete Publisher command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    await publisher?.close()
    this.publishers.delete(extendedPublisherId)
    this.logger.info(`deleted publisher with publishing id ${publisherId}`)
    return res.ok
  }

  /**
   * Declare a consumer for a stream
   *
   * Consumers receive messages from a stream starting at a specified offset.
   * They can be configured with various options including single active consumer mode,
   * filtering, and custom credit policies.
   *
   * @param params - Consumer configuration including stream, offset, and optional settings
   * @param handle - Message handler function that processes received messages
   * @param superStreamConsumer - Optional super stream consumer for internal use
   * @returns A promise that resolves to a Consumer instance
   * @throws {Error} If the consumer cannot be declared or filtering is not supported
   * @example
   * ```typescript
   * // Basic consumer
   * const consumer = await client.declareConsumer(
   *   { stream: 'my-stream', offset: Offset.first() },
   *   (message) => console.log(message.content.toString())
   * );
   *
   * // Single active consumer with offset tracking
   * const sacConsumer = await client.declareConsumer(
   *   {
   *     stream: 'my-stream',
   *     offset: Offset.first(),
   *     singleActive: true,
   *     consumerRef: 'my-consumer-group',
   *     consumerUpdateListener: async (ref, stream) => {
   *       const offset = await client.queryOffset({ reference: ref, stream });
   *       return Offset.offset(offset);
   *     }
   *   },
   *   (message) => console.log(message.content.toString())
   * );
   * ```
   */
  public async declareConsumer(
    params: DeclareConsumerParams,
    handle: ConsumerFunc,
    superStreamConsumer?: SuperStreamConsumer
  ): Promise<Consumer> {
    const connection = await this.getConnection(params.stream, "consumer", params.connectionClosedListener)
    const consumerId = connection.getNextConsumerId()

    if (params.filter && !connection.isFilteringEnabled) {
      throw new Error(`Broker does not support message filtering.`)
    }

    const consumer = new StreamConsumer(
      this.pool,
      handle,
      {
        connection,
        stream: params.stream,
        consumerId,
        consumerRef: params.consumerRef,
        consumerTag: params.consumerTag,
        offset: params.offset,
        creditPolicy: params.creditPolicy,
        singleActive: params.singleActive,
        consumerUpdateListener: params.consumerUpdateListener,
      },
      params.filter
    )
    connection.registerForCloseConsumer(consumer.extendedId, params.stream, async () => {
      if (params.connectionClosedListener) {
        params.connectionClosedListener(false)
      }
      await this.closeConsumer(consumer.extendedId)
    })
    this.consumers.set(consumer.extendedId, { connection, consumer, params })
    await this.declareConsumerOnConnection(params, consumerId, connection, superStreamConsumer?.superStream)
    this.logger.info(
      `New consumer created with stream name ${params.stream}, consumer id ${consumerId} and offset ${params.offset.type}`
    )
    return consumer
  }

  public async closeConsumer(extendedConsumerId: string) {
    const activeConsumer = this.consumers.get(extendedConsumerId)
    if (!activeConsumer) {
      this.logger.error("Consumer does not exist")
      throw new Error(`Consumer with id: ${extendedConsumerId} does not exist`)
    }

    const consumerId = extractConsumerId(extendedConsumerId)
    const { streamInfos } = await this.locatorConnection.sendAndWait<MetadataResponse>(
      new MetadataRequest({ streams: [activeConsumer.consumer.streamName] })
    )
    if (streamInfos.length > 0 && streamExists(streamInfos[0])) {
      await this.unsubscribe(activeConsumer.connection, consumerId)
    }
    await this.closing(activeConsumer.consumer, extendedConsumerId)
    return true
  }

  public async declareSuperStreamConsumer(
    { superStream, offset, consumerRef, creditPolicy }: DeclareSuperStreamConsumerParams,
    handle: SuperStreamConsumerFunc
  ): Promise<SuperStreamConsumer> {
    const partitions = await this.queryPartitions({ superStream })
    return SuperStreamConsumer.create(handle, {
      superStream,
      locator: this,
      consumerRef: consumerRef || `${superStream}-${randomUUID()}`,
      offset: offset || Offset.first(),
      partitions,
      creditPolicy,
    })
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

  public queryOffset(params: QueryOffsetParams) {
    return this.locatorConnection.queryOffset(params)
  }

  private async closeAllConsumers() {
    await Promise.all([...this.consumers.values()].map(({ consumer }) => consumer.close()))
    this.consumers = new Map<string, ConsumerMappedValue>()
  }

  private async closeAllPublishers() {
    await Promise.all([...this.publishers.values()].map((c) => c.publisher.close()))
    this.publishers = new Map<string, PublisherMappedValue>()
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

  /**
   * Create a new stream on the RabbitMQ server
   *
   * @param params - Stream configuration including name and optional arguments (max-length-bytes, max-age, etc.)
   * @returns A promise that resolves to true when the stream is created or already exists
   * @throws {Error} If the create command fails with an error other than "already exists"
   * @example
   * ```typescript
   * // Simple stream
   * await client.createStream({ stream: 'my-stream' });
   *
   * // Stream with retention policy
   * await client.createStream({
   *   stream: 'my-stream',
   *   arguments: {
   *     'max-length-bytes': 10_000_000_000, // 10GB
   *     'max-age': '7D' // 7 days
   *   }
   * });
   * ```
   */
  public async createStream(params: { stream: string; arguments?: CreateStreamArguments }): Promise<true> {
    this.logger.debug(`Create Stream...`)
    const res = await this.locatorConnection.sendAndWait<CreateStreamResponse>(new CreateStreamRequest(params))
    if (res.code === STREAM_ALREADY_EXISTS_ERROR_CODE) {
      return true
    }
    if (!res.ok) {
      throw new Error(`Create Stream command returned error with code ${res.code}`)
    }

    this.logger.debug(`Create Stream response: ${res.ok} - with arguments: '${inspect(params.arguments)}'`)
    return res.ok
  }

  /**
   * Delete a stream from the RabbitMQ server
   *
   * @param params - Parameters containing the stream name to delete
   * @returns A promise that resolves to true when the stream is deleted
   * @throws {Error} If the delete command fails
   */
  public async deleteStream(params: { stream: string }): Promise<true> {
    this.logger.debug(`Delete Stream...`)
    const res = await this.locatorConnection.sendAndWait<DeleteStreamResponse>(new DeleteStreamRequest(params.stream))
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
    const res = await this.locatorConnection.sendAndWait<CreateSuperStreamResponse>(
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
    const res = await this.locatorConnection.sendAndWait<DeleteSuperStreamResponse>(
      new DeleteSuperStreamRequest(params.streamName)
    )
    if (!res.ok) {
      throw new Error(`Delete Super Stream command returned error with code ${res.code}`)
    }
    this.logger.debug(`Delete Super Stream response: ${res.ok} - '${inspect(params.streamName)}'`)
    return res.ok
  }

  public async streamStatsRequest(streamName: string) {
    const res = await this.locatorConnection.sendAndWait<StreamStatsResponse>(new StreamStatsRequest(streamName))
    if (!res.ok) {
      throw new Error(`Stream Stats command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Statistics for stream name ${streamName}, ${res.statistics}`)
    return res.statistics
  }

  public getConnectionInfo(): ConnectionInfo {
    return this.locatorConnection.getConnectionInfo()
  }

  public async subscribe(params: SubscribeParams): Promise<SubscribeResponse> {
    const res = await this.locatorConnection.sendAndWait<SubscribeResponse>(new SubscribeRequest({ ...params }))
    if (!res.ok) {
      throw new Error(`Subscribe command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    return res
  }

  /**
   * Restart the client after a connection failure
   *
   * This method re-establishes all connections (locator, publishers, and consumers)
   * and re-declares all publishers and consumers. Useful for automatic reconnection
   * after network failures.
   *
   * @returns A promise that resolves when all connections are restarted
   * @example
   * ```typescript
   * const client = await connect({
   *   // ...connection params
   *   listeners: {
   *     connection_closed: (hadError) => {
   *       client.restart()
   *         .then(() => console.log('Reconnected'))
   *         .catch(err => console.error('Reconnection failed', err));
   *     }
   *   }
   * });
   * ```
   */
  public async restart() {
    this.logger.info(`Restarting client connection ${this.locatorConnection.connectionId}`)
    const uniqueConnectionIds = new Set<string>()
    uniqueConnectionIds.add(this.locatorConnection.connectionId)

    await wait(5000)
    await this.locatorConnection.restart()

    for (const { consumer, connection, params } of this.consumers.values()) {
      if (!uniqueConnectionIds.has(connection.connectionId)) {
        this.logger.info(`Restarting consumer connection ${connection.connectionId}`)
        await connection.restart()
      }
      uniqueConnectionIds.add(connection.connectionId)
      const consumerParams = { ...params, offset: Offset.offset(consumer.getOffset()) }
      await this.declareConsumerOnConnection(consumerParams, consumer.consumerId, connection)
    }

    for (const { publisher, connection, params, filter } of this.publishers.values()) {
      if (!uniqueConnectionIds.has(connection.connectionId)) {
        this.logger.info(`Restarting publisher connection ${connection.connectionId}`)
        await connection.restart()
      }
      uniqueConnectionIds.add(connection.connectionId)
      await this.declarePublisherOnConnection(params, publisher.publisherId, connection, filter)
    }
  }

  public get maxFrameSize() {
    return this.locatorConnection.maxFrameSize ?? DEFAULT_FRAME_MAX
  }

  public get serverVersions() {
    return this.locatorConnection.serverVersions
  }

  public get rabbitManagementVersion() {
    return this.locatorConnection.rabbitManagementVersion
  }

  public async routeQuery(params: { routingKey: string; superStream: string }) {
    const res = await this.locatorConnection.sendAndWait<RouteResponse>(new RouteQuery(params))
    if (!res.ok) {
      throw new Error(`Route Query command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Route Response for super stream ${params.superStream}, ${res.streams}`)
    return res.streams
  }

  public async partitionsQuery(params: { superStream: string }) {
    const res = await this.locatorConnection.sendAndWait<PartitionsResponse>(new PartitionsQuery(params))
    if (!res.ok) {
      throw new Error(`Partitions Query command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    this.logger.info(`Partitions Response for super stream ${params.superStream}, ${res.streams}`)
    return res.streams
  }

  private async declarePublisherOnConnection(
    params: DeclarePublisherParams,
    publisherId: number,
    connection: Connection,
    filter?: FilterFunc
  ) {
    const res = await connection.sendAndWait<DeclarePublisherResponse>(
      new DeclarePublisherRequest({ stream: params.stream, publisherRef: params.publisherRef, publisherId })
    )
    if (!res.ok) {
      await connection.close()
      throw new Error(`Declare Publisher command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    if (filter && !connection.isFilteringEnabled) {
      throw new Error(`Broker does not support message filtering.`)
    }
  }

  private async declareConsumerOnConnection(
    params: DeclareConsumerParams,
    consumerId: number,
    connection: Connection,
    superStream?: string
  ) {
    const properties: Record<string, string> = {}
    if (params.singleActive && !params.consumerRef) {
      throw new Error("consumerRef is mandatory when declaring a single active consumer")
    }
    if (params.singleActive) {
      properties["single-active-consumer"] = "true"
      properties["name"] = params.consumerRef!
    }
    if (superStream) {
      properties["super-stream"] = superStream
    }
    if (params.filter) {
      for (let i = 0; i < params.filter.values.length; i++) {
        properties[`filter.${i}`] = params.filter.values[i]
      }
      properties["match-unfiltered"] = `${params.filter.matchUnfiltered}`
    }
    if (params.consumerTag) {
      properties["identifier"] = params.consumerTag
    }

    const creditPolicy = params.creditPolicy || defaultCreditPolicy

    const res = await connection.sendAndWait<SubscribeResponse>(
      new SubscribeRequest({
        ...params,
        subscriptionId: consumerId,
        credit: creditPolicy.onSubscription(),
        properties: properties,
      })
    )

    if (!res.ok) {
      this.consumers.delete(computeExtendedConsumerId(consumerId, connection.connectionId))
      throw new Error(`Declare Consumer command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
  }

  private askForCredit(subscriptionId: number, connection: Connection): CreditRequestWrapper {
    return async (howMany: number) => {
      return connection.send(new CreditRequest({ subscriptionId: subscriptionId, credit: howMany }))
    }
  }

  private getDeliverV1Callback(connectionId: string) {
    return async (response: DeliverResponse) => {
      const deliverData = {
        messages: response.messages,
        subscriptionId: response.subscriptionId,
        consumerId: computeExtendedConsumerId(response.subscriptionId, connectionId),
        messageFilteringSupported: false,
      }
      await this.handleDelivery(deliverData)
    }
  }

  private getDeliverV2Callback(connectionId: string) {
    return async (response: DeliverResponseV2) => {
      const deliverData = {
        messages: response.messages,
        subscriptionId: response.subscriptionId,
        consumerId: computeExtendedConsumerId(response.subscriptionId, connectionId),
        messageFilteringSupported: true,
      }
      await this.handleDelivery(deliverData)
    }
  }

  private handleDelivery = async (deliverData: DeliverData) => {
    const { messages, subscriptionId, consumerId, messageFilteringSupported } = deliverData
    const { consumer, connection } = this.consumers.get(consumerId) ?? {
      consumer: undefined,
      connection: undefined,
    }
    if (!consumer) {
      this.logger.error(`On delivery, no consumer found`)
      return
    }
    this.logger.debug(`on delivery -> ${consumer.consumerRef}`)
    this.logger.debug(`response.messages.length: ${messages.length}`)

    const creditRequestWrapper = this.askForCredit(subscriptionId, connection)
    await consumer.creditPolicy.onChunkReceived(creditRequestWrapper)
    const messageFilter =
      messageFilteringSupported && consumer.filter?.postFilterFunc
        ? consumer.filter?.postFilterFunc
        : (_msg: Message) => true

    for (const message of messages) {
      if (messageFilter(message)) {
        await consumer.handle(message)
      }
    }

    await consumer.creditPolicy.onChunkCompleted(creditRequestWrapper)
  }

  private getConsumerUpdateCallback(connectionId: string) {
    return async (response: ConsumerUpdateQuery) => {
      const { consumer, connection } = this.consumers.get(
        computeExtendedConsumerId(response.subscriptionId, connectionId)
      ) ?? {
        consumer: undefined,
        connection: undefined,
      }
      if (!consumer) {
        this.logger.error(`On consumer_update_query no consumer found`)
        return
      }
      const offset = await this.getConsumerOrServerSavedOffset(consumer)
      consumer.updateConsumerOffset(offset)
      this.logger.debug(`on consumer_update_query -> ${consumer.consumerRef}`)
      await connection.send(
        new ConsumerUpdateResponse({ correlationId: response.correlationId, responseCode: 1, offset })
      )
    }
  }

  private async getConsumerOrServerSavedOffset(consumer: StreamConsumer) {
    if (consumer.isSingleActive && consumer.consumerRef && consumer.consumerUpdateListener) {
      try {
        const offset = await consumer.consumerUpdateListener(consumer.consumerRef, consumer.streamName)
        return offset
      } catch (error) {
        this.logger.error(
          `Error in consumerUpdateListener for consumerRef ${consumer.consumerRef}: ${(error as Error).message}`
        )
        return consumer.offset
      }
    }

    return consumer.offset
  }

  private getLocatorConnection() {
    const connectionParams = this.buildConnectionParams(false, "", this.params.listeners?.connection_closed)
    return Connection.create(connectionParams, this.logger)
  }

  private async getConnection(
    streamName: string,
    purpose: ConnectionPurpose,
    connectionClosedListener?: ConnectionClosedListener
  ): Promise<Connection> {
    const [metadata] = await this.queryMetadata({ streams: [streamName] })
    const isPublisher = purpose === "publisher"
    const chosenNode = chooseNode(metadata, isPublisher)
    if (!chosenNode) {
      throw new Error(`Stream was not found on any node`)
    }
    const connection = await this.pool.getConnection(
      purpose,
      streamName,
      this.locatorConnection.vhost,
      chosenNode.host,
      async () => {
        return await this.getConnectionOnChosenNode(
          isPublisher,
          streamName,
          chosenNode,
          metadata,
          connectionClosedListener
        )
      }
    )
    return connection
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
  ): ConnectionParams {
    const connectionId = randomUUID()
    const connectionListeners = {
      ...this.params.listeners,
      connection_closed: connectionClosedListener,
      deliverV1: this.getDeliverV1Callback(connectionId),
      deliverV2: this.getDeliverV2Callback(connectionId),
      consumer_update_query: this.getConsumerUpdateCallback(connectionId),
    }
    return {
      ...this.params,
      listeners: connectionListeners,
      leader: leader,
      streamName: streamName,
      connectionId,
    }
  }

  private async getConnectionOnChosenNode(
    isPublisher: boolean,
    streamName: string,
    chosenNode: { host: string; port: number },
    metadata: StreamMetadata,
    connectionClosedListener?: ConnectionClosedListener
  ): Promise<Connection> {
    const connectionParams = this.buildConnectionParams(isPublisher, streamName, connectionClosedListener)
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

  private async unsubscribe(connection: Connection, consumerId: number) {
    const res = await connection.sendAndWait<UnsubscribeResponse>(new UnsubscribeRequest(consumerId))
    if (!res.ok) {
      throw new Error(`Unsubscribe command returned error with code ${res.code} - ${errorMessageOf(res.code)}`)
    }
    return res
  }

  private async closing(consumer: StreamConsumer, extendedConsumerId: string) {
    await consumer.close()
    this.consumers.delete(extendedConsumerId)
    this.logger.info(`Closed consumer with id: ${extendedConsumerId}`)
  }

  /**
   * Create and connect a new Client instance
   *
   * @param params - Connection parameters including hostname, port, credentials, and optional settings
   * @param logger - Optional logger instance for debugging
   * @returns A promise that resolves to a connected Client instance
   * @example
   * ```typescript
   * const client = await Client.connect({
   *   hostname: 'localhost',
   *   port: 5552,
   *   username: 'guest',
   *   password: 'guest',
   *   vhost: '/'
   * });
   * ```
   */
  static async connect(params: ClientParams, logger?: Logger): Promise<Client> {
    return new Client(logger ?? new NullLogger(), {
      ...params,
      vhost: getVhostOrDefault(params.vhost),
    }).start()
  }
}

/**
 * Listener callbacks for client events
 */
export type ClientListenersParams = {
  metadata_update?: MetadataUpdateListener
  publish_confirm?: ConnectionPublishConfirmListener
  publish_error?: ConnectionPublishErrorListener
  connection_closed?: ConnectionClosedListener
}

/**
 * TLS/SSL connection parameters for secure connections
 */
export interface SSLConnectionParams {
  key?: string
  cert?: string
  ca?: string
  rejectUnauthorized?: boolean
}

/**
 * Configuration for load balancer/address resolver mode
 */
export type AddressResolverParams =
  | {
      enabled: true
      endpoint?: { host: string; port: number }
    }
  | { enabled: false }

/**
 * Configuration parameters for connecting to RabbitMQ
 */
export interface ClientParams {
  hostname: string
  port: number
  username: string
  password: string
  mechanism?: "PLAIN" | "EXTERNAL"
  vhost: string
  frameMax?: number
  heartbeat?: number
  listeners?: ClientListenersParams
  ssl?: SSLConnectionParams | boolean
  bufferSizeSettings?: BufferSizeSettings
  socketTimeout?: number
  addressResolver?: AddressResolverParams
  leader?: boolean
  streamName?: string
  connectionName?: string
}

/**
 * Parameters for declaring a publisher
 */
export interface DeclarePublisherParams {
  /** Name of the stream to publish to */
  stream: string
  /** Optional reference for deduplication - if provided, enables publishing ID tracking */
  publisherRef?: string
  /** Optional maximum chunk length for batching */
  maxChunkLength?: number
  /** Optional callback when the publisher's connection closes */
  connectionClosedListener?: ConnectionClosedListener
}

/**
 * Routing strategy for super stream publishers
 */
export type RoutingStrategy = "key" | "hash"

/**
 * Parameters for declaring a super stream publisher
 */
export interface DeclareSuperStreamPublisherParams {
  superStream: string
  publisherRef?: string
  routingStrategy?: RoutingStrategy
}

/**
 * Function to filter messages on the client side
 */
export type MessageFilter = (msg: Message) => boolean

/**
 * Configuration for message filtering (both server-side and client-side)
 */
export interface ConsumerFilter {
  /** Filter values for server-side bloom filter */
  values: string[]
  /** Optional client-side post-filter function */
  postFilterFunc: MessageFilter
  /** Whether to match messages without filter tags */
  matchUnfiltered: boolean
}

/**
 * Parameters for declaring a consumer
 */
export interface DeclareConsumerParams {
  stream: string
  consumerRef?: string
  offset: Offset
  connectionClosedListener?: ConnectionClosedListener
  consumerUpdateListener?: ConsumerUpdateListener
  singleActive?: boolean
  filter?: ConsumerFilter
  creditPolicy?: ConsumerCreditPolicy
  consumerTag?: string
}

export interface DeclareSuperStreamConsumerParams {
  superStream: string
  consumerRef?: string
  offset?: Offset
  creditPolicy?: ConsumerCreditPolicy
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

/**
 * Connect to RabbitMQ and create a new Client instance
 *
 * This is the main entry point for creating a connection to RabbitMQ streams.
 * It establishes a locator connection for metadata queries and sets up connection pooling.
 *
 * @param params - Connection parameters including hostname, port, credentials, and optional settings
 * @param logger - Optional logger instance for debugging and monitoring
 * @returns A promise that resolves to a connected Client instance
 * @throws {Error} If connection fails (invalid credentials, network issues, etc.)
 * @example
 * ```typescript
 * // Basic connection
 * const client = await connect({
 *   hostname: 'localhost',
 *   port: 5552,
 *   username: 'guest',
 *   password: 'guest',
 *   vhost: '/'
 * });
 *
 * // Connection with TLS
 * const secureClient = await connect({
 *   hostname: 'rabbitmq.example.com',
 *   port: 5551,
 *   username: 'user',
 *   password: 'pass',
 *   vhost: '/',
 *   ssl: {
 *     cert: fs.readFileSync('client-cert.pem'),
 *     key: fs.readFileSync('client-key.pem'),
 *     ca: fs.readFileSync('ca-cert.pem')
 *   }
 * });
 *
 * // Connection with listeners
 * const client = await connect({
 *   hostname: 'localhost',
 *   port: 5552,
 *   username: 'guest',
 *   password: 'guest',
 *   vhost: '/',
 *   listeners: {
 *     connection_closed: (hadError) => {
 *       console.log('Connection closed', hadError);
 *     },
 *     publish_confirm: (confirm, connId) => {
 *       console.log('Message confirmed', confirm.publishingId);
 *     }
 *   }
 * });
 * ```
 */
export function connect(params: ClientParams, logger?: Logger): Promise<Client> {
  return Client.connect(params, logger)
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

const extractConsumerId = (extendedConsumerId: string) => {
  return parseInt(extendedConsumerId.split("@").shift() ?? "0")
}

const extractPublisherId = (extendedPublisherId: string) => {
  return parseInt(extendedPublisherId.split("@").shift() ?? "0")
}

const getVhostOrDefault = (vhost: string) => vhost ?? "/"

const streamExists = (streamInfo: StreamMetadata): boolean => {
  return (
    streamInfo.responseCode !== ResponseCode.StreamDoesNotExist &&
    streamInfo.responseCode !== ResponseCode.SubscriptionIdDoesNotExist
  )
}
