/*
    Run this example only with rabbit management version >= 3.13.0.

    Shows how to resume a super stream consumer from the offsets stored on the server.
    resolveStartOffset is passed as the consumerUpdateListener: it is called for every partition
    with the partition stream name when that partition's consumer becomes active.
*/

const rabbit = require("rabbitmq-stream-js-client")
const { randomUUID } = require("crypto")

const rabbitUser = process.env.RABBITMQ_USER || "rabbit"
const rabbitPassword = process.env.RABBITMQ_PASSWORD || "rabbit"

async function consumeFromStoredOffsets(client, superStream, consumerRef, label) {
  const received = []
  const consumer = await client.declareSuperStreamConsumer(
    {
      superStream,
      consumerRef,
      // the stored offset + 1 when the server has one for the partition, otherwise the start of the partition
      consumerUpdateListener: (ref, partition) =>
        rabbit.resolveStartOffset(client, rabbit.Offset.first(), ref, partition),
    },
    async (message, partitionConsumer) => {
      received.push(message.content.toString())
      await partitionConsumer.storeOffset(message.offset)
    }
  )
  await sleep(1000)
  console.log(`${label} consumer received ${received.length} messages: ${received.sort().join(", ")}`)
  await consumer.close()
}

async function publish(client, superStream, messages) {
  const publisher = await client.declareSuperStreamPublisher(
    { superStream },
    (content, msgOptions) => msgOptions.messageProperties.messageId
  )
  for (const body of messages) {
    await publisher.send(Buffer.from(body), { messageProperties: { messageId: body } })
  }
  await publisher.close()
}

async function main() {
  const superStream = `example-${randomUUID()}`
  const consumerRef = "resume-example"
  console.log(`Creating super stream ${superStream}`)

  const client = await rabbit.connect({
    hostname: "localhost",
    port: 5552,
    username: rabbitUser,
    password: rabbitPassword,
    vhost: "/",
    heartbeat: 0,
  })
  await client.createSuperStream({ streamName: superStream })
  await sleep(200) // Waiting for partitions to be created

  await publish(client, superStream, ["message 1", "message 2", "message 3", "message 4", "message 5", "message 6"])
  // no offset is stored yet: every partition starts from the beginning and all six messages are received
  await consumeFromStoredOffsets(client, superStream, consumerRef, "first")

  await publish(client, superStream, ["message 7", "message 8"])
  // every partition resumes after its own stored offset: only the two new messages are received
  await consumeFromStoredOffsets(client, superStream, consumerRef, "second")

  await client.deleteSuperStream({ streamName: superStream })
  await client.close()
}

main()
  .then(() => console.log("done!"))
  .catch((res) => {
    console.log("ERROR ", res)
    process.exit(-1)
  })
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
