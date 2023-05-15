const rabbit = require("rabbitmq-stream-js-client")
const amqplib = require("amqplib")
const { randomUUID } = require("crypto")

const rabbitUser = process.env.RABBITMQ_USER || "rabbit"
const rabbitPassword = process.env.RABBITMQ_PASSWORD || "rabbit"

async function main() {
  const streamName = `example-${randomUUID()}`
  console.log(`Create stream ${streamName}`)
  console.log("AAAA", process.env.RABBITMQ_USER, process.env.RABBITMQ_PASSWORD)
  const connection = await rabbit.connect({
    hostname: "localhost",
    port: 5552,
    username: rabbitUser,
    password: rabbitPassword,
    vhost: "/",
    heartbeat: 0,
  })
  await connection.createStream({ stream: streamName, arguments: {} })
  const producer = await connection.declarePublisher({ stream: streamName })

  await producer.send(BigInt(1), Buffer.from("ciao"))

  await createClassicConsumer(streamName)

  await connection.deleteStream({ stream: streamName })

  await connection.close()
}

/**
 *
 * @param {string} queueName name of the stream to consume
 * @returns {Promise<void>}
 */
async function createClassicConsumer(queueName) {
  return new Promise(async (res, rej) => {
    try {
      const conn = await amqplib.connect(`amqp://${rabbitUser}:${rabbitPassword}@localhost`)
      const ch = await conn.createChannel()
      await ch.prefetch(1000)
      await ch.consume(
        queueName,
        async (msg) => {
          if (!msg) {
            return
          }

          console.log("ACK", msg.content.toString())
          ch.ack(msg)
          if (msg.content.toString() === "ciao") {
            await ch.close()
            await conn.close()
            res()
          }
        },
        { arguments: { "x-stream-offset": "first" } }
      )
    } catch (err) {
      rej(err)
    }
  })
}

main()
  .then(() => console.log("done!"))
  .catch((res) => {
    console.log("ERROR ", res)
    process.exit(-1)
  })
