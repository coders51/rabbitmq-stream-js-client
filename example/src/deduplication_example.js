/*
    Run this example only with rabbit management version >= 3.13.0.
*/

const rabbit = require("rabbitmq-stream-js-client")
const { randomUUID } = require("crypto")

const rabbitUser = process.env.RABBITMQ_USER || "rabbit"
const rabbitPassword = process.env.RABBITMQ_PASSWORD || "rabbit"

async function main() {
  const streamName = `example-${randomUUID()}`
  const publisherRef = `publisher-${randomUUID()}`
  console.log(`Creating stream ${streamName}`)

  const client = await rabbit.connect({
    hostname: "localhost",
    port: 5552,
    username: rabbitUser,
    password: rabbitPassword,
    vhost: "/",
    heartbeat: 0,
  })
  await client.createStream({ stream: streamName })
  await sleep(200)

  const firstDeduplicationPublisher = await client.declarePublisher({ stream: streamName, publisherRef: publisherRef })
  await firstDeduplicationPublisher.send(Buffer.from("Test message 1"))
  await firstDeduplicationPublisher.send(Buffer.from("Test message 2"))
  await firstDeduplicationPublisher.send(Buffer.from("Test message 3"))
  await firstDeduplicationPublisher.flush()
  const firstPublisherPublishingId = await firstDeduplicationPublisher.getLastPublishingId()
  await firstDeduplicationPublisher.close()

  console.log(`Publishing id is ${firstPublisherPublishingId}`)

  const secondDeduplicationPublisher = await client.declarePublisher({ stream: streamName, publisherRef: publisherRef })
  await secondDeduplicationPublisher.send(Buffer.from("Test message 1"))
  await secondDeduplicationPublisher.send(Buffer.from("Test message 2"))
  await secondDeduplicationPublisher.flush()
  const secondPublisherPublishingId = await secondDeduplicationPublisher.getLastPublishingId()
  await secondDeduplicationPublisher.close()

  console.log(`Publishing id is still ${secondPublisherPublishingId} (same as first ${firstPublisherPublishingId})`)

  await client.deleteStream({ stream: streamName })

  await client.close()
}

main()
  .then(() => console.log("done!"))
  .catch((res) => {
    console.log("ERROR ", res)
    process.exit(-1)
  })
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
