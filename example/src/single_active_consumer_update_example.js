const rabbit = require("rabbitmq-stream-js-client")
const crypto = require("crypto")

const wait = (ms) => new Promise((r) => setTimeout(r, ms))

async function main() {
  const messagesFromFirstConsumer = []
  const messagesFromSecondConsumer = []

  console.log("Connecting...")
  const client = await rabbit.connect({
    vhost: "/",
    port: 5552,
    hostname: "localhost",
    username: "rabbit",
    password: "rabbit",
  })

  console.log("Making sure the stream exists...")
  const streamName = "stream-single-active-consumer-update-javascript"
  await client.createStream({ stream: streamName, arguments: {} })
  const consumerRef = `my-consumer-${crypto.randomUUID()}`

  console.log("Creating the publisher and sending 100 messages...")
  const publisher = await client.declarePublisher({ stream: streamName })
  for (let i = 1; i <= 100; i++) {
    await publisher.send(Buffer.from(`${i}`))
  }

  console.log("Creating the first consumer, when 50 messages are consumed it saves the offset on the server...")
  const consumer1 = await client.declareConsumer(
    {
      stream: streamName,
      offset: rabbit.Offset.first(),
      singleActive: true,
      consumerRef: consumerRef,
      consumerUpdateListener: async (consumerReference, streamName) => {
        const offset = await client.queryOffset({ reference: consumerReference, stream: streamName })
        return rabbit.Offset.offset(offset)
      },
    },
    async (message) => {
      messagesFromFirstConsumer.push(`Message ${message.content.toString("utf-8")} from ${consumerRef}`)
      if (messagesFromFirstConsumer.length === 50) {
        await consumer1.storeOffset(message.offset)
      }
    }
  )

  await wait(500)

  console.log("Creating the second consumer, when it becomes active it resumes from the stored offset on the server...")
  await client.declareConsumer(
    {
      stream: streamName,
      offset: rabbit.Offset.first(),
      singleActive: true,
      consumerRef: consumerRef,
      // This callback is executed when the consumer becomes active
      consumerUpdateListener: async (consumerReference, streamName) => {
        const offset = await client.queryOffset({ reference: consumerReference, stream: streamName })
        return rabbit.Offset.offset(offset)
      },
    },
    (message) => {
      messagesFromSecondConsumer.push(`Message ${message.content.toString("utf-8")} from ${consumerRef}`)
    }
  )

  console.log("Closing the first consumer to trigger the activation of the second one...")
  await client.closeConsumer(consumer1.extendedId)

  await wait(500)

  console.log(`Messages consumed by the first consumer: ${messagesFromFirstConsumer.length}`)
  console.log(
    `Messages consumed by the second consumer: ${messagesFromSecondConsumer.length}`,
    messagesFromSecondConsumer
  )
}

main()
  .then(() => {
    console.log("done!")
    process.exit(0)
  })
  .catch((res) => {
    console.log("Error in publishing message!", res)
    process.exit(-1)
  })
