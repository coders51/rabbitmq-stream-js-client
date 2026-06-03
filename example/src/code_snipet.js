const rabbit = require("rabbitmq-stream-js-client")
const amqplib = require("amqplib")

async function amqplibSetupAndSend(username, password, port) {
  const connection = await amqplib.connect(`amqp://${username}:${password}@localhost:${port}/`)
  const channel = await connection.createChannel()

  channel.sendToQueue("debug_work_queue.dlq", Buffer.from("gap0YXNrTnVtYmVyoTA="), {
    headers: {
      attemptID: 1,
      chainIndex: 1,
      A: "hell.world",
      ID: 55,
      placedInDLQ: 1764156981485,
      recursiveDepth: 1,
      taskIdentifier: "107",
      timestamp: 1764156978730,
      "x-death": [
        {
          count: 1,
          reason: "expired",
          queue: "delay-level-02-queue",
          time: { "!": "timestamp", value: 1764180998 },
          exchange: "delay-level-02-exchange",
          "routing-keys": ["0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.1.0.0"],
        },
      ],
      "x-first-death-exchange": "delay-level-02-exchange",
      "x-first-death-queue": "delay-level-02-queue",
      "x-first-death-reason": "expired",
      "x-last-death-exchange": "delay-level-02-exchange",
      "x-last-death-queue": "delay-level-02-queue",
      "x-last-death-reason": "expired",
      "x-stream-filter-value": "55_1764156978",
      "x-stream-offset": 30000,
    },
    deliveryMode: 2,
    priority: 8,
    messageId: "7ebdbb2c-5726-49ae-b24f-335158f04f1c",
    type: "message_type",
    "x-stream-filter-value": "55_1764156978",
    "x-stream-offset": 30771,
  })

  await channel.close()
  await connection.close()
}

async function main() {
  console.log("hello world")
  const client = await rabbit.connect(
    {
      hostname: "localhost",
      port: 5552,
      username: "rabbit",
      password: "rabbit",
      vhost: "/",
    },
    {
      debug: (message) => console.log(`RMQ DEBUG:`, message),
      info: (message) => console.log(`RMQ INFO:`, message),
      warn: (message) => console.log(`RMQ WARN:`, message),
      error: (message) => console.log(`RMQ ERROR:`, message),
    }
  )
  const streamQueue = "debug_work_queue.dlq"
  await client.createStream({ stream: streamQueue, arguments: { "max-age": "14D" } })

  await client.declareConsumer(
    { stream: streamQueue, consumerRef: "debug_consumer", offset: rabbit.Offset.first() },
    (msg) => {
      console.log("consumed message:", msg.content.toString())
      console.log(`Properties : ${JSON.stringify(msg.messageProperties)}`)
      // console.log(`Headers : ${JSON.stringify(msg.messageProperties?.headers)}`);
    }
  )

  await amqplibSetupAndSend("rabbit", "rabbit", 5672)
}

main()
  .then(() => console.log("DONE"))
  .catch((e) => {
    console.error(e)
    process.exit(1)
  })
