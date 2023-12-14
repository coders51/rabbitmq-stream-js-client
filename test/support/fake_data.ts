import { randomUUID } from "crypto"
import { Connection, ListenersParams, connect } from "../../src/connection"
import { MessageProperties } from "../../src/producer"

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

export async function createPublisher(streamName: string, connection: Connection) {
  const publisher = await connection.declarePublisher({
    stream: streamName,
    publisherRef: `my-publisher-${randomUUID()}`,
  })
  return publisher
}

export function createConnection(username: string, password: string, listeners?: ListenersParams, frameMax?: number) {
  return connect({
    hostname: "localhost",
    port: 5552,
    username,
    password,
    vhost: "/",
    frameMax: frameMax ?? 0, // not used
    heartbeat: 0,
    listeners: listeners,
  })
}
