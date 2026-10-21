/*
    Run this example only with rabbit management version >= 3.13.0.
*/

const rabbit = require("rabbitmq-stream-js-client")
const promises = require("timers/promises")
const { randomUUID } = require("crypto")

const rabbitUser = process.env.RABBITMQ_USER || "rabbit"
const rabbitPassword = process.env.RABBITMQ_PASSWORD || "rabbit"

async function main() {
  const superStreamName = `example-${randomUUID()}`
  console.log(`Creating super stream ${superStreamName}`)

  const client = await rabbit.connect({
    hostname: "localhost",
    port: 5552,
    username: rabbitUser,
    password: rabbitPassword,
    vhost: "/",
    heartbeat: 0,
  })
  await client.createSuperStream({ streamName: superStreamName })
  await sleep(200) // Waiting for partitions to be created

  // the routingKeyExtractor is only used if you do not provide a routingKey when sending a message
  const routingKeyExtractor = (content, msgOptions) => msgOptions.messageProperties.messageId
  const publisher = await client.declareSuperStreamPublisher(
    {
      superStream: superStreamName,
      routingStrategy: "hash",
    },
    routingKeyExtractor
  )

  // send messages one at a time
  await publisher.send(Buffer.from("Test message F:1"), { routingKey: "F" })
  await publisher.send(Buffer.from("Test message B:1"), { routingKey: "B" })
  await publisher.send(Buffer.from("Test message B:2"), { routingKey: "B" })
  await publisher.send(Buffer.from("Test message A:1"), { messageProperties: { messageId: "A" } })

  await promises.setImmediate()

  // or send them in a batch
  await publisher.sendSubEntries([
    { content: Buffer.from("Test message B:3"), routingKey: "B" },
    { content: Buffer.from("Test message B:4"), routingKey: "B" },
    { content: Buffer.from("Test message A:2"), routingKey: "A" },
  ])

  // note how the messages arrive IN order by partition (the routingKey values above were carefully chosen to demonstrate this)
  // but messages from different partitions may interleave with each other
  await client.declareSuperStreamConsumer({ superStream: superStreamName }, (message, consumer) => {
    console.log(`Received message ${message.content.toString()} from partition: ${consumer.streamName}`)
  })

  await sleep(2000)

  await client.close()
}

main()
  .then(() => console.log("done!"))
  .catch((res) => {
    console.log("ERROR ", res)
    process.exit(-1)
  })
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
