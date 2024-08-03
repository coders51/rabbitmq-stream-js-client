const rabbit = require("rabbitmq-stream-js-client")

const rabbitUser = process.env.RABBITMQ_USER || "rabbit"
const rabbitPassword = process.env.RABBITMQ_PASSWORD || "rabbit"

async function main() {
  const streamName = `stream-offset-tracking-javascript`
  console.log(`Creating stream ${streamName}`)

  const client = await rabbit.connect({
    hostname: "localhost",
    port: 5552,
    username: rabbitUser,
    password: rabbitPassword,
    vhost: "/",
    heartbeat: 0,
  })
  await client.createStream({ stream: streamName, arguments: {} })
  const publisher = await client.declarePublisher({ stream: streamName })
  const messageCount = 100

  console.log(`Publishing ${messageCount} messages`)
  for (let i = 0; i < messageCount; i++) {
    const body = i === messageCount - 1 ? "marker" : `hello ${i}`
    await publisher.send(Buffer.from(body))
  }

  const startFrom = rabbit.Offset.offset(0n)
  let firstOffset = startFrom.value
  let lastOffset = startFrom.value
  let messageReceivedCount = 0
  const consumerRef = "offset-tracking-tutorial"
  const consumer = await client.declareConsumer({ stream: streamName, offset: startFrom, consumerRef }, (message) => {
    messageReceivedCount++
    if (message.offset === startFrom.value) {
      console.log("First message received")
      firstOffset = message.offset
    }
    if (messageReceivedCount % 10 === 0) {
      console.log("Storing offset")
      client.storeOffset({ stream: streamName, reference: consumerRef, offsetValue: message.offset })
    }
    if (message.content.toString() === "marker") {
      console.log("Marker found")
      client.storeOffset({ stream: streamName, reference: consumerRef, offsetValue: message.offset })
      lastOffset = message.offset
    }
  })

  console.log(`Start consuming...`)
  await sleep(2000)
  console.log(`Done consuming, first offset was ${firstOffset}, last offset was ${lastOffset}`)
  const lastStoredOffset = await consumer.queryOffset()
  console.log(`Last stored offset was ${lastStoredOffset}`)

  await client.close()
}

main()
  .then(() => console.log("done!"))
  .catch((res) => {
    console.log("ERROR ", res)
    process.exit(-1)
  })
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
