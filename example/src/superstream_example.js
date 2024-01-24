/*
    Run this example only with rabbit management version >= 3.13.0.
*/

const rabbit = require("rabbitmq-stream-js-client")
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

  const routingKeyExtractor = (content, msgOptions) => msgOptions.messageProperties.messageId
  const publisher = await client.declareSuperStreamPublisher({ superStream: superStreamName }, routingKeyExtractor)

  await publisher.send(Buffer.from("Test message 1"), { messageProperties: { messageId: "1" } })
  await publisher.send(Buffer.from("Test message 2"), { messageProperties: { messageId: "2" } })
  await publisher.send(Buffer.from("Test message 3"), { messageProperties: { messageId: "3" } })

  await client.declareSuperStreamConsumer({ superStream: superStreamName }, (message) => {
    console.log(`Received message ${message.content.toString()}`)
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
