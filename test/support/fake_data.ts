import { randomUUID } from "crypto"
import { Client, ListenersParams, connect } from "../../src/client"
import { MessageProperties } from "../../src/producer"
import { BufferSizeSettings } from "../../src/requests/request"

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

export function createStreamName() {
  return `my-stream-${randomUUID()}`
}

export async function createPublisher(streamName: string, client: Client) {
  const publisher = await client.declarePublisher({
    stream: streamName,
    publisherRef: `my-publisher-${randomUUID()}`,
  })
  return publisher
}

export function createClient(
  username: string,
  password: string,
  listeners?: ListenersParams,
  frameMax?: number,
  bufferSizeSettings?: BufferSizeSettings
) {
  return connect({
    hostname: "localhost",
    port: 5552,
    username,
    password,
    vhost: "/",
    frameMax: frameMax ?? 0,
    heartbeat: 0,
    listeners: listeners,
    bufferSizeSettings: bufferSizeSettings,
  })
}
