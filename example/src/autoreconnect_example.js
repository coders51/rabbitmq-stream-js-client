const rabbit = require("rabbitmq-stream-js-client")
const { randomUUID } = require("crypto")

const rabbitUser = process.env.RABBITMQ_USER || "rabbit"
const rabbitPassword = process.env.RABBITMQ_PASSWORD || "rabbit"

async function main() {
  const streamName = `example-${randomUUID()}`
  console.log(`Creating stream ${streamName}`)

  let client = undefined

  const connectToRabbit = async () => {
    client = await rabbit.connect({
      hostname: "localhost",
      port: 5553,
      username: rabbitUser,
      password: rabbitPassword,
      listeners: {
        connection_closed: async () => {
          await sleep(Math.random() * 3000)
          connectToRabbit()
            .then(() => console.log("Successfully re-connected to rabbit!"))
            .catch((e) => console.error("Error while reconnecting to Rabbit!", e))
        },
      },
      vhost: "/",
      heartbeat: 0,
    })
  }

  await connectToRabbit()

  await sleep(2000)

  console.log("Closing!")
  await client.close()
  console.log("Now it should reopen!")

  await sleep(10000)
}

main()
  .then(() => console.log("done!"))
  .catch((res) => {
    console.log("ERROR ", res)
    process.exit(-1)
  })
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
