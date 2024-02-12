const rabbit = require("rabbitmq-stream-js-client")
const { randomUUID } = require("crypto")
const { exec } = require("child_process")
const { promisify, inspect } = require("util")
const promiseExec = promisify(exec)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

const rabbitUser = process.env.RABBITMQ_USER || "rabbit"
const rabbitPassword = process.env.RABBITMQ_PASSWORD || "rabbit"
let client = undefined
let publisher = undefined
let consumer = undefined
let consumerOffset = rabbit.Offset.first()
let streamName = `example-${randomUUID()}`
const publisherOutbox = { messageIds: [], messages: new Map(), publishingIds: new Map(), offset: 0 }
const received = new Set()
let publishingId = 1n
let restartCount = 0
let published = 0
let confirmed = 0
let callbackCalled = 0
const logger = {
  debug: (msg) => { return },
  info: (msg) => console.log(`[info]\t[${new Date().toISOString()}] ${msg}`),
  error: (msg) => console.log(`[error]\t[${new Date().toISOString()}] ${msg}`),
  warn: (msg) => console.log(`[warn]\t[${new Date().toISOString()}] ${msg}`),
}


function getNodesFromEnv() {
  const envValue = process.env.RABBIT_MQ_TEST_NODES ?? "localhost:5552"
  const nodes = envValue.split(";")
  return nodes.map((n) => {
    const [host, port] = n.split(":")
    return { host: host ?? "localhost", port: parseInt(port) ?? 5552 }
  })
}

async function triggerConnectionIssue() {
  const res = await promiseExec("cd .. && docker-compose restart")
  return true
}

function generateMessage() {
  const payload = Buffer.from(`${randomUUID()}`)
  const messageId = `${randomUUID()}`
  return { payload, messageId }
}

function scheduleMessageProduction() {
  setInterval(() => {
    const { payload, messageId } = generateMessage()
    publisherOutbox.messageIds.push(messageId)
    publisherOutbox.messages.set(messageId, payload)
  }, 50)
}

async function scheduleMessageDelivery() {
  setInterval(async () => {
    const messageOffset = publisherOutbox.offset
    const oldestMessageId = publisherOutbox.messageIds[messageOffset]
    const { writable } = publisher?.getConnectionInfo() ?? false
    if (publisher && writable && oldestMessageId !== undefined) {
      const message = publisherOutbox.messages.get(oldestMessageId)
      const res = await publisher.send(message, { messageProperties: { messageId: `${oldestMessageId}` } })
      published++
      publisherOutbox.offset++
      if (res.publishingId !== undefined) {
        publisherOutbox.publishingIds.set(res.publishingId, oldestMessageId)
      }
    }
  }, 10)
}

function scheduleLogInfo() {
  setInterval(() => {
    logger.info(`outbox queue length: ${publisherOutbox.messageIds.length} offset ${publisherOutbox.offset}`)
    logger.info(`${inspect({ published, confirmed, received: received.size })}`)
    logger.info(`client local port: ${inspect(client && client.getConnectionInfo().localPort)} consumer local port: ${inspect(consumer && consumer.getConnectionInfo().localPort)} publisher local port: ${inspect(publisher && publisher.getConnectionInfo().localPort)}`)
  }, 3000)
}

async function triggerConnectionIssues() {
  return new Promise((res, rej) => {
    setInterval(async () => {
      logger.info("Closing!")
      restartCount++
      await triggerConnectionIssue()
      if (restartCount >= 1000) {
        try {
          logger.info("Terminating...")
          if (client) await client.close()
          res(true)
          return
        }
        catch (e) {
          rej(e)
          return
        }
      }
      logger.info("\nNow it should reopen!\n")
    }, 60000)
  })
}

async function setupPublisher(client) {
  const publisherRef = `publisher - ${randomUUID()}`
  const publisherConfig = { stream: streamName, publisherRef: publisherRef, connectionClosedListener: (err) => { return } }
  const publisherConfirmCallback = (err, confirmedIds) => {
    if (err) {
      logger.info(`Publish confirm error ${inspect(err)} `)
      return
    }
    confirmed = confirmed + confirmedIds.length
    confirmedMessageIds = confirmedIds.map((publishingId) => {
      const messageId = publisherOutbox.publishingIds.get(publishingId)
      publisherOutbox.publishingIds.delete(publishingId)
      return messageId
    })

    publisherOutbox.messageIds = publisherOutbox.messageIds.filter((id) => {
      return !confirmedMessageIds.includes(id)
    })
    confirmedMessageIds.forEach((id) => {
      if (publisherOutbox.messages.delete(id)) {
        publisherOutbox.offset = publisherOutbox.offset - 1
      }
    })
  }
  const publisher = await client.declarePublisher(publisherConfig)
  publisher.on("publish_confirm", publisherConfirmCallback)
  publisherOutbox.offset = 0
  return publisher
}

async function setupConsumer(client) {
  const consumerConfig = { stream: streamName, offset: rabbit.Offset.timestamp(new Date()), connectionClosedListener: (err) => { return } }
  const receiveCallback = (msg) => {
    const msgId = msg.messageProperties.messageId
    if (received.has(msgId)) {
      //in this example the consumer restarts from the last handled message.
      //@see https://blog.rabbitmq.com/posts/2021/09/rabbitmq-streams-offset-tracking/
      //and Consumer.storeOffset and Consumer.queryOffset for a more complete approach
      logger.info(`dedup: ${msgId}`)
    }
    received.add(msgId)
    consumerOffset = msg.offset
    return
  }
  return client.declareConsumer(consumerConfig, receiveCallback)
}

async function setup() {
  try {
    const connectionClosedCallback = () => {
      logger.info(`In connection closed event...`)
      callbackCalled++
      if (callbackCalled > 10) {
        process.exit(0)
      }
      client.restart().then(() => {
        logger.info(`Connections restarted!`)
      }).catch((reason) => {
        logger.warn(`Could not reconnect to Rabbit! ${reason}`)
      })
    }
    const firstNode = getNodesFromEnv()[0]
    logger.info(`Now invoking rabbit.connect on ${inspect(firstNode)}`)
    client = await rabbit.connect({
      hostname: firstNode.host,
      port: firstNode.port,
      username: rabbitUser,
      password: rabbitPassword,
      listeners: { connection_closed: connectionClosedCallback, },
      vhost: "/",
      heartbeat: 0,
    }, logger)
    await client.createStream({ stream: streamName, arguments: {} })
    publisher = await setupPublisher(client)
    consumer = await setupConsumer(client)
    return { client, publisher, consumer }
  }
  catch (err) {
    logger.warn(`Setup-wide error: ${inspect(err)}`)
  }
}

async function main() {
  await setup()
  scheduleMessageProduction()
  await scheduleMessageDelivery()
  scheduleLogInfo()
  await triggerConnectionIssues()
}

main()
  .then(() => logger.info("done!"))
  .catch((res) => {
    logger.info("ERROR ", res)
    process.exit(-1)
  })

