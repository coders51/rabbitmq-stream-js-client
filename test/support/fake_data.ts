import { randomUUID } from "crypto"
import { Client, ClientListenersParams, connect } from "../../src/client"
import { MessageProperties } from "../../src/publisher"
import { BufferSizeSettings } from "../../src/requests/request"
import { Offset } from "../../src/requests/subscribe_request"
import { Consumer, Publisher } from "../../src"
import { getTestNodesFromEnv } from "./util"
import { createLogger, format, transports } from "winston"
import { inspect } from "util"

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

export function createConsumerRef(): string {
  return `my-consumer-${randomUUID()}`
}

export async function createPublisher(streamName: string, client: Client): Promise<Publisher> {
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
  listeners?: ClientListenersParams,
  frameMax?: number,
  bufferSizeSettings?: BufferSizeSettings,
  port?: number,
  connectionName?: string
): Promise<Client> {
  const [firstNode] = getTestNodesFromEnv()
  return connect(
    {
      hostname: firstNode.host,
      port: port ?? firstNode.port,
      username,
      password,
      vhost: "/",
      frameMax: frameMax ?? 0,
      heartbeat: 0,
      listeners: listeners,
      bufferSizeSettings: bufferSizeSettings,
      connectionName: connectionName,
    }
    // testLogger
  )
}

export const testLogger = createLogger({
  level: "debug",
  format: format.combine(
    format.colorize(),
    format.timestamp(),
    format.align(),
    format.splat(),
    format.label(),
    format.printf((info) => `${info.timestamp} ${info.level}: ${info.message} ${info.meta ? inspect(info.meta) : ""}`)
  ),
  transports: new transports.Console(),
})
