import { randomUUID } from "crypto"
import { Client, ListenersParams, connect } from "../../src/client"
import { MessageProperties, Producer } from "../../src/producer"
import { BufferSizeSettings } from "../../src/requests/request"
import { Offset } from "../../src/requests/subscribe_request"
import { Consumer } from "../../src"
import { getTestNodesFromEnv } from "../../src/util"

export function createProperties(): MessageProperties {
  return {
    contentType: `contentType`,
    contentEncoding: `contentEncoding`,
    replyTo: `replyTo`,
    to: `to`,
    subject: `subject`,
    correlationId: `correlationIdAAA`,
    messageId: `messageId`,
    userId: Buffer.from(`userId`),
    absoluteExpiryTime: new Date(),
    creationTime: new Date(),
    groupId: `groupId`,
    groupSequence: 666,
    replyToGroupId: `replyToGroupId`,
  }
}

export function createStreamName(): string {
  return `my-stream-${randomUUID()}`
}

export async function createPublisher(streamName: string, client: Client): Promise<Producer> {
  const publisher = await client.declarePublisher({
    stream: streamName,
    publisherRef: `my-publisher-${randomUUID()}`,
  })
  return publisher
}

export async function createConsumer(streamName: string, client: Client): Promise<Consumer> {
  const id = randomUUID()
  const consumer = await client.declareConsumer(
    { stream: streamName, offset: Offset.first(), consumerRef: `my-consumer-${id}` },
    () => {
      console.log(`Test consumer with id ${id} received a message`)
    }
  )
  return consumer
}

export async function createClient(
  username: string,
  password: string,
  listeners?: ListenersParams,
  frameMax?: number,
  bufferSizeSettings?: BufferSizeSettings
): Promise<Client> {
  const [firstNode] = getTestNodesFromEnv()
  return connect({
    hostname: firstNode.host,
    port: firstNode.port,
    username,
    password,
    vhost: "/",
    frameMax: frameMax ?? 0,
    heartbeat: 0,
    listeners: listeners,
    bufferSizeSettings: bufferSizeSettings,
  })
}
