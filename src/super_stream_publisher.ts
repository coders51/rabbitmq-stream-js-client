import { Client, RoutingStrategy } from "./client"
import { murmur32 } from "./hash/murmur32"
import { MessageOptions, Publisher } from "./publisher"
import { bigIntMax } from "./util"

export type MessageKeyExtractorFunction = (content: string, opts: MessageOptions) => string | undefined

type SuperStreamPublisherParams = {
  locator: Client
  superStream: string
  publisherRef?: string
  routingStrategy?: RoutingStrategy
  keyExtractor: MessageKeyExtractorFunction
}

export class SuperStreamPublisher {
  private locator: Client
  private partitions: string[] = []
  private publishers: Map<string, Publisher> = new Map()
  private superStream: string
  private publisherRef: string | undefined
  private keyExtractor: MessageKeyExtractorFunction
  private routingStrategy: RoutingStrategy

  private constructor(params: SuperStreamPublisherParams) {
    this.locator = params.locator
    this.publisherRef = params.publisherRef
    this.superStream = params.superStream
    this.routingStrategy = params.routingStrategy ?? "hash"
    this.keyExtractor = params.keyExtractor
  }

  static async create(params: SuperStreamPublisherParams): Promise<SuperStreamPublisher> {
    const superStreamPublisher = new SuperStreamPublisher(params)
    await superStreamPublisher.start()
    return superStreamPublisher
  }

  public async start(): Promise<void> {
    this.partitions = await this.locator.queryPartitions({ superStream: this.superStream })
  }

  public async close(): Promise<void> {
    await Promise.all([...this.publishers.values()].map((p) => p.close()))
    this.publishers = new Map()
  }

  public async send(message: Buffer, opts: MessageOptions): Promise<boolean> {
    const partition = await this.routeMessage(message, opts)
    const publisher = await this.getPublisher(partition)
    return publisher.send(message, opts)
  }

  public async basicSend(publishingId: bigint, message: Buffer, opts: MessageOptions): Promise<boolean> {
    const partition = await this.routeMessage(message, opts)
    const publisher = await this.getPublisher(partition)
    return publisher.basicSend(publishingId, message, opts)
  }

  public async getLastPublishingId(): Promise<bigint> {
    return bigIntMax(await Promise.all([...this.publishers.values()].map((p) => p.getLastPublishingId()))) ?? 0n
  }

  private async routeMessage(messageContent: Buffer, msg: MessageOptions): Promise<string> {
    const routingKey = this.keyExtractor(messageContent.toString(), msg)
    if (!routingKey) {
      throw new Error(`Routing key is empty or undefined with the provided extractor`)
    }
    if (this.routingStrategy === "hash") {
      const hash = murmur32(routingKey)
      const partitionIndex = hash % this.partitions.length
      return this.partitions[partitionIndex]!
    } else {
      const targetPartitions = await this.locator.routeQuery({ routingKey, superStream: this.superStream })
      if (!targetPartitions.length) {
        throw new Error(`The server did not return any partition for routing key: ${routingKey}`)
      }
      for (const tp of targetPartitions) {
        const foundPartition = this.partitions.find((p) => p === tp)
        if (foundPartition) return foundPartition
      }
      throw new Error(
        `Key routing strategy failed: server returned partitions ${targetPartitions} but no match was found`
      )
    }
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
