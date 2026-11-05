/*
    Run this example only with rabbit management version >= 3.13.0.

    Demonstrates passing a routingKey directly when sending a message to a super stream,
    instead of (or in addition to) relying on the routingKeyExtractor.
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

  // send messages one at a time, passing the routingKey explicitly
  // for this example, the routingKey values were chosen after experimentation to ensure distribution across all 3 partitions
  await publisher.send(Buffer.from("Test message F:1"), { routingKey: "F" })
  await publisher.send(Buffer.from("Test message B:1"), { routingKey: "B" })
  await publisher.send(Buffer.from("Test message B:2"), { routingKey: "B" })

  // no routingKey provided: falls back to the routingKeyExtractor (messageId here)
  await publisher.send(Buffer.from("Test message A:1"), { messageProperties: { messageId: "A" } })

  await promises.setImmediate()

  // sendSubEntries also accepts a routingKey per message
  await publisher.sendSubEntries([
    { content: Buffer.from("Test message B:3"), routingKey: "B" },
    { content: Buffer.from("Test message B:4"), routingKey: "B" },
    { content: Buffer.from("Test message A:2"), routingKey: "A" },
  ])

  // messages sharing a routingKey land in the same partition and arrive in order;
  // eg:  A -> 1, B -> 2 and F -> 0
  // eg: messages for B will all arrive in 1,2,3,4 order but possibly interleaved with messages from a different partition.
  await client.declareSuperStreamConsumer({ superStream: superStreamName }, (message, consumer) => {
    console.log(`Received message ${message.content.toString()} from partition: ${consumer.streamName}`)
  })

  // sample output:
  /*
Creating super stream example-uuid
Received message Test message B:1 from partition: example-uuid-2
Received message Test message B:2 from partition: example-uuid-2
Received message Test message A:1 from partition: example-uuid-1
Received message Test message F:1 from partition: example-uuid-0
Received message Test message B:3 from partition: example-uuid-2
Received message Test message B:4 from partition: example-uuid-2
Received message Test message A:2 from partition: example-uuid-1
done!
*/

  // this shows that the B messages were received in order 1,2,3,4 but A:1 and F:1 interleaved between them
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
