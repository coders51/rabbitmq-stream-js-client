import { createLogger, format, transports } from "winston"
import { connect } from "rabbitmq-stream-js-client"
import { randomUUID } from "crypto"
import { argv } from "process"
import { PerfTestPublisher } from "./perf_test_publisher"
import { inspect } from "util"
import { BufferSizeSettings } from "../dist/requests/request"

const logger = createLogger({
  level: "info",
  format: format.combine(
    format.colorize(),
    format.timestamp(),
    format.align(),
    format.splat(),
    format.label(),
    format.printf((info) => `${info.timestamp} ${info.level}: ${info.message} ${info.meta ? inspect(info.meta) : ""}`)
  ),
  transports: new transports.Console(),
})

const connLogger = undefined

function parseArgs(args) {
  const zip = (a: string[], b: string[]): [string, number][] => {
    const shorterArray = a.length < b.length ? a : b
    const zipped = shorterArray.map((_, i) => [a[i], +b[i]] as [string, number])
    return zipped
  }
  const orderedNamedArgs = ["maxMessages", "messageSize"]
  const defaultNamedArgs = {
    maxMessages: 100000,
    messageSize: 10,
  }
  const passedNamedArgs = Object.fromEntries(zip(orderedNamedArgs, args))
  return { ...defaultNamedArgs, ...passedNamedArgs }
}

async function main() {
  const rabbitUser = process.env.RABBITMQ_USER || "rabbit"
  const rabbitPassword = process.env.RABBITMQ_PASSWORD || "rabbit"
  const bufferSizeSettings: BufferSizeSettings = { initialSize: 16384 }
  const frameMax = 65536

  const client = await connect(
    {
      hostname: "localhost",
      port: 5552,
      username: rabbitUser,
      password: rabbitPassword,
      bufferSizeSettings: bufferSizeSettings,
      vhost: "/",
      frameMax,
    },
    connLogger
  )

  const streamName = `my-stream-${randomUUID()}`
  await client.createStream({ stream: streamName, arguments: {} })
  const publisherRef = `my-publisher-${randomUUID()}`
  const passedArgs = parseArgs(argv.slice(2))
  logger.info(
    `Stream: ${streamName} - publisher ${publisherRef} - max messages ${passedArgs.maxMessages} - message size: ${
      passedArgs.messageSize
    } bytes - write buffer settings: ${inspect(bufferSizeSettings)}`
  )

  const perfTestPublisher = new PerfTestPublisher(
    client,
    logger,
    passedArgs.maxMessages,
    { stream: streamName, publisherRef: publisherRef },
    passedArgs.messageSize
  )
  logger.info(`${new Date().toISOString()} - cycle start`)
  await perfTestPublisher.cycle()
}

main()
  .then((_v) => setTimeout(() => process.exit(0), 1000))
  .catch((res) => {
    logger.error("ERROR ", res)
    process.exit(400)
  })
