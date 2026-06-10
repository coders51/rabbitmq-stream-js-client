import { Client, RoutingStrategy } from "./client"
import { CompressionType } from "./compression"
import { murmur32 } from "./hash/murmur32"
import { Message, MessageOptions, Publisher, SendResult } from "./publisher"
import { bigIntMax } from "./util"

/**
 * Extracts the routing key from a message
 *
 * The returned key determines which partition of the super stream the message
 * is published to. Returning `undefined` causes the send to fail.
 *
 * @param content - The message content as a string
 * @param opts - The options the message is being sent with
 * @returns The routing key, or `undefined` if none can be derived
 */
export type MessageKeyExtractorFunction = (content: string, opts: MessageOptions) => string | undefined

type SuperStreamPublisherParams = {
  locator: Client
  superStream: string
  publisherRef?: string
  routingStrategy?: RoutingStrategy
  keyExtractor: MessageKeyExtractorFunction
}

/**
 * Publishes messages to a super stream
 *
 * A super stream is a logical stream made of several partitions, each one a
 * regular stream. The publisher routes every message to a partition using a
 * routing key (extracted via a {@link MessageKeyExtractorFunction}) and the
 * configured routing strategy, lazily creating an underlying {@link Publisher}
 * per partition as needed.
 */
export class SuperStreamPublisher {
  private locator: Client
  private partitions: string[] = []
  private publishers: Map<string, Publisher> = new Map()
  private superStream: string
  private publisherRef: string | undefined
  private keyExtractor: MessageKeyExtractorFunction
  private routingStrategy: RoutingStrategy
  private routingCache: Map<string, string> = new Map()

  private constructor(params: SuperStreamPublisherParams) {
    this.locator = params.locator
    this.publisherRef = params.publisherRef
    this.superStream = params.superStream
    this.routingStrategy = params.routingStrategy ?? "hash"
    this.keyExtractor = params.keyExtractor
  }

  /**
   * Create and start a super stream publisher
   *
   * Queries the super stream partitions so the publisher is ready to route messages.
   */
  static async create(params: SuperStreamPublisherParams): Promise<SuperStreamPublisher> {
    const superStreamPublisher = new SuperStreamPublisher(params)
    await superStreamPublisher.start()
    return superStreamPublisher
  }

  /** Query and cache the partitions of the super stream */
  public async start(): Promise<void> {
    this.partitions = await this.locator.queryPartitions({ superStream: this.superStream })
  }

  /** Close every underlying partition publisher */
  public async close(): Promise<void> {
    await Promise.all([...this.publishers.values()].map((p) => p.close()))
    this.publishers = new Map()
  }

  /**
   * Route and publish a message to the appropriate partition
   *
   * @param message - The message content
   * @param opts - Options used both for sending and for extracting the routing key
   */
  public async send(message: Buffer, opts: MessageOptions): Promise<SendResult> {
    const partition = await this.routeMessage(message, opts)
    const publisher = await this.getPublisher(partition)
    return publisher.send(message, opts)
  }

  /**
   * Route and publish a message with an explicit publishing id (for deduplication)
   *
   * @param publishingId - The publishing id to assign to the message
   * @param message - The message content
   * @param opts - Options used both for sending and for extracting the routing key
   */
  public async basicSend(publishingId: bigint, message: Buffer, opts: MessageOptions): Promise<SendResult> {
    const partition = await this.routeMessage(message, opts)
    const publisher = await this.getPublisher(partition)
    return publisher.basicSend(publishingId, message, opts)
  }

  /**
   * Route a batch of messages to their partitions and publish them as sub-batch entries
   *
   * Messages are grouped by partition before being sent, optionally compressed.
   *
   * @param messages - The messages to publish
   * @param compressionType - The compression to apply to each sub-batch (default: none)
   */
  public async sendSubEntries(messages: Message[], compressionType: CompressionType = CompressionType.None) {
    // route all messages
    const messagesByPartition: Map<string, Message[]> = new Map()
    await Promise.all(
      messages.map(async (m) => {
        const partition = await this.routeMessage(m.content, m)
        let msgs = messagesByPartition.get(partition)
        if (!msgs) {
          msgs = []
          messagesByPartition.set(partition, msgs)
        }
        msgs.push(m)
      })
    )

    // init all publishers, in sequence in order to avoid instantiating two publishers for the same node
    const partitions = [...messagesByPartition.keys()]
    for (const p of partitions) {
      await this.getPublisher(p)
    }

    // send all messages in parallel
    await Promise.all(
      partitions.map(async (p) => {
        const pub = await this.getPublisher(p)
        return pub.sendSubEntries(messagesByPartition.get(p) ?? [], compressionType)
      })
    )
  }

  /**
   * Get the highest publishing id across all partition publishers
   *
   * @returns The maximum last publishing id, or `0n` if no message has been published yet
   */
  public async getLastPublishingId(): Promise<bigint> {
    return bigIntMax(await Promise.all([...this.publishers.values()].map((p) => p.getLastPublishingId()))) ?? 0n
  }

  private async routeMessage(messageContent: Buffer, msg: MessageOptions): Promise<string> {
    const routingKey = this.keyExtractor(messageContent.toString(), msg)
    if (!routingKey) {
      throw new Error(`Routing key is empty or undefined with the provided extractor`)
    }
    let partition = this.routingCache.get(routingKey)
    if (!partition) {
      if (this.routingStrategy === "hash") {
        const hash = murmur32(routingKey)
        const partitionIndex = hash % this.partitions.length
        partition = this.partitions[partitionIndex]!
      } else {
        const targetPartitions = await this.locator.routeQuery({ routingKey, superStream: this.superStream })
        if (!targetPartitions.length) {
          throw new Error(`The server did not return any partition for routing key: ${routingKey}`)
        }
        partition = targetPartitions.find((tp) => this.partitions.find((p) => p === tp))
        if (!partition) {
          throw new Error(
            `Key routing strategy failed: server returned partitions ${targetPartitions} but no match was found`
          )
        }
      }
    }
    this.routingCache.set(routingKey, partition)
    return partition
  }

  private async getPublisher(partition: string): Promise<Publisher> {
    const publisher = this.publishers.get(partition)
    if (publisher) {
      return publisher
    }
    return this.initPublisher(partition)
  }

  private async initPublisher(partition: string): Promise<Publisher> {
    const publisher = await this.locator.declarePublisher({ stream: partition, publisherRef: this.publisherRef })
    this.publishers.set(partition, publisher)
    return publisher
  }
}
