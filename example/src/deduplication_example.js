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

  //to declare a publisher with deduplication enabled, you need to set a publisherRef
  const firstDeduplicationPublisher = await client.declarePublisher({ stream: streamName, publisherRef: publisherRef })

  //with deduplication actived, you can send messages without a publishingId; in this case it will be incremental
  await firstDeduplicationPublisher.send(Buffer.from("Test message 1")) //publishingId = 1
  await firstDeduplicationPublisher.send(Buffer.from("Test message 2")) //publishingId = 2
  //but you can also set a publishingId, note that it must be greater than the last one for the message to be stored
  await firstDeduplicationPublisher.send(Buffer.from("Test message 3"), { publishingId: 3n }) //publishingId = 3
  //if you choose a publishingId that is less than the last one, the message will not be stored
  await firstDeduplicationPublisher.send(Buffer.from("Test message 4"), { publishingId: 1n }) //this message won't be stored
  await firstDeduplicationPublisher.flush()
  const firstPublisherPublishingId = await firstDeduplicationPublisher.getLastPublishingId()
  await firstDeduplicationPublisher.close()

  console.log(`Publishing id is ${firstPublisherPublishingId} (must be 3)`) //this must be the greatest publishingId, 3 in this case

  const secondDeduplicationPublisher = await client.declarePublisher({ stream: streamName, publisherRef: publisherRef })
  //with the second publisher if we try to send messages with lower publishingId than the last one, they will not be stored
  await secondDeduplicationPublisher.send(Buffer.from("Test message 5"), { publishingId: 1n }) //won't be stored
  await secondDeduplicationPublisher.send(Buffer.from("Test message 6"), { publishingId: 2n }) //won't be stored
  await secondDeduplicationPublisher.send(Buffer.from("Test message 7"), { publishingId: 7n }) //this will be stored since 7 is greater than 3, the last highest publishingId
  await secondDeduplicationPublisher.flush()
  const secondPublisherPublishingId = await secondDeduplicationPublisher.getLastPublishingId()
  await secondDeduplicationPublisher.close()

  console.log(`Publishing id is ${secondPublisherPublishingId} (must be 7)`) //this must be the greatest publishingId, 7 in this case

  await client.deleteStream({ stream: streamName })

  await client.close()
}

main()
  .then(() => console.log("done!"))
  .catch((res) => {
    console.log("ERROR ", res)
    process.exit(-1)
  })
