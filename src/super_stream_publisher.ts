import { Client } from "./client"
import { murmurHash } from "./murmur"
import { MessageOptions, Publisher } from "./publisher"

export type MessageKeyExtractorFunction = (content: string, opts: MessageOptions) => string | undefined

type SuperStreamPublisherParams = {
  locator: Client
  superStream: string
  keyExtractor: MessageKeyExtractorFunction
}

export class SuperStreamPublisher {
  private locator: Client
  private partitions: string[] = []
  private publishers: Map<string, Publisher> = new Map()
  private superStream: string
  private keyExtractor: MessageKeyExtractorFunction
  private readonly murmur32Seed = 104729 //  must be the same to all the clients to be compatible

  private constructor(params: SuperStreamPublisherParams) {
    this.locator = params.locator
    this.superStream = params.superStream
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

  private async routeMessage(messageContent: Buffer, msg: MessageOptions): Promise<string> {
    const routingKey = this.keyExtractor(messageContent.toString(), msg)
    if (!routingKey) {
      throw new Error(`Routing key is empty or undefined with the provided extractor`)
    }
    const hash = murmurHash(this.murmur32Seed)(routingKey)
    const partitionIndex = hash % this.partitions.length
    const partition = this.partitions[partitionIndex]!
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
    const publisher = await this.locator.declarePublisher({ stream: partition })
    this.publishers.set(partition, publisher)
    return publisher
  }
}
