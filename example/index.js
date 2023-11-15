const rabbit = require("rabbitmq-stream-js-client")
const { randomUUID } = require("crypto")
const { Offset } = require("../dist/requests/subscribe_request")

const rabbitUser = process.env.RABBITMQ_USER || "rabbit"
const rabbitPassword = process.env.RABBITMQ_PASSWORD || "rabbit"

async function main() {
  const streamName = `example-${randomUUID()}`
  console.log(`Create stream ${streamName}`)

  const client = await rabbit.connect({
    hostname: "localhost",
    port: 5552,
    username: rabbitUser,
    password: rabbitPassword,
    vhost: "/",
    heartbeat: 0,
  })
  await client.createStream({ stream: streamName, arguments: {} })
  const producer = await client.declarePublisher({ stream: streamName })

  await producer.send(Buffer.from("ciao"))

  await connection.declareConsumer({ stream: streamName, offset: Offset.first() }, (message) => {
    console.log(`Received message ${message.content.toString()}`)
  })
}

main()
  .then(() => console.log("done!"))
  .catch((res) => {
    console.log("ERROR ", res)
    process.exit(-1)
  })
