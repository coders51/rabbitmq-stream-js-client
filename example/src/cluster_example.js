/*
    Run this example only after creating the cluster.
    Following the indications at https://github.com/coders51/rabbitmq-stream-js-client/tree/main/cluster
*/

const rabbit = require("rabbitmq-stream-js-client")
const { randomUUID } = require("crypto")

const rabbitUser = process.env.RABBITMQ_USER || "rabbit"
const rabbitPassword = process.env.RABBITMQ_PASSWORD || "rabbit"

async function main() {
  const streamName = `example-${randomUUID()}`
  console.log(`Creating stream ${streamName}`)

  const client = await rabbit.connect({
    hostname: "node0",
    port: 5562,
    username: rabbitUser,
    password: rabbitPassword,
    vhost: "/",
    heartbeat: 0,
    addressResolver: { enabled: true, endpoint: { host: "localhost", port: 5553 } },
  })
  await client.createStream({ stream: streamName, arguments: {} })
  await sleep(200) // Waiting for replicas to be created
  const publisher = await client.declarePublisher({ stream: streamName })

  await publisher.send(Buffer.from("Test message"))

  await client.declareConsumer({ stream: streamName, offset: rabbit.Offset.first() }, (message) => {
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
