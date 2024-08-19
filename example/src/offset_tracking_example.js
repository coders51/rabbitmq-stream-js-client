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
  const toSend = 100

  console.log(`Publishing ${toSend} messages`)
  for (let i = 0; i < toSend; i++) {
    const body = i === toSend - 1 ? "marker" : `hello ${i}`
    await publisher.send(Buffer.from(body))
  }

  const consumerRef = "offset-tracking-tutorial"
  let firstOffset = undefined
  let offsetSpecification = rabbit.Offset.first()
  try {
    const offset = await client.queryOffset({ reference: consumerRef, stream: streamName })
    offsetSpecification = rabbit.Offset.offset(offset + 1n)
  } catch (e) {}

  let lastOffset = offsetSpecification.value
  let messageCount = 0
  const consumer = await client.declareConsumer(
    { stream: streamName, offset: offsetSpecification, consumerRef },
    async (message) => {
      messageCount++
      if (!firstOffset && messageCount === 1) {
        firstOffset = message.offset
        console.log("First message received")
      }
      if (messageCount % 10 === 0) {
        await consumer.storeOffset(message.offset)
      }
      if (message.content.toString() === "marker") {
        console.log("Marker found")
        lastOffset = message.offset
        await consumer.storeOffset(message.offset)
        console.log(`Done consuming, first offset was ${firstOffset}, last offset was ${lastOffset}`)
        await consumer.close(true)
        process.exit(0)
      }
    }
  )

  console.log(`Start consuming...`)
  await sleep(2000)
}

main()
  .then(() => console.log("done!"))
  .catch((res) => {
    console.log("ERROR ", res)
    process.exit(-1)
  })
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
