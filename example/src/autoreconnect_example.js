/*

  Auto-reconnect example: mitigate simple network disconnection issues

  In this example we assume that:
    - the stream topology does not change (i.e. leader/replicas nodes do not change)
    - hostnames/ip addresses do not change
    - the connection_closed event is triggered on the TCP connection used by the Client instance

  The example is composed of
    - message generation part (mimicks the behavior of a client application)
    - toy outbox pattern implementation (in-memory structure, no persistence of data, etc.)
    - a client instance with a registered callback on the connection_closed event
    - scheduled delivery of messages through a producer
    - very simple publish_confirm handling
    - one consumer
    - a scheduled reachability interruption (in this case obtained by launching `docker-compose restart`)
    - a scheduled process that logs the state of the application (connections, message counters)
*/

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

/*
  very simple message generation function
*/
function generateMessage() {
  const payload = Buffer.from(`${randomUUID()}`)
  const messageId = `${randomUUID()}`
  return { payload, messageId }
}

/*
  at each iteration, a new message is put in the outbox.
  This mimicks a client application that generates messages to be sent.
*/
function scheduleMessageProduction() {
  setInterval(() => {
    const { payload, messageId } = generateMessage()
    publisherOutbox.messageIds.push(messageId)
    publisherOutbox.messages.set(messageId, payload)
  }, 50)
}

/*
  at each iteration, a new message is read from the outbox and sent using the publisher.
  Note that the operation is executed only if 
  there is a new message to be sent and if the publisher connection is at least established.
  If the publisher is not `ready`, then the message will be cached internally.
*/
async function scheduleMessageDelivery() {
  setInterval(async () => {
    //keep track of the last message sent (but not yet confirmed)
    const messageOffset = publisherOutbox.offset
    const oldestMessageId = publisherOutbox.messageIds[messageOffset]
    //is the publisher socket open?
    const { writable } = publisher?.getConnectionInfo() ?? false
    if (publisher && writable && oldestMessageId !== undefined) {
      const message = publisherOutbox.messages.get(oldestMessageId)
      const res = await publisher.send(message, { messageProperties: { messageId: `${oldestMessageId}` } })
      published++
      publisherOutbox.offset++
      if (res.publishingId !== undefined) {
        //keep track of the messageId, by mapping it with the protocol-generated publishingId
        publisherOutbox.publishingIds.set(res.publishingId, oldestMessageId)
      }
    }
  }, 10)
}

/*
  at each interval, the state of the outbox, the message counters and the state of client connections will be logged. 
 */
function scheduleLogInfo() {
  setInterval(() => {
    logger.info(`outbox queue length: ${publisherOutbox.messageIds.length} offset ${publisherOutbox.offset}`)
    logger.info(`${inspect({ published, confirmed, received: received.size })}`)
    logger.info(`client local port: ${inspect(client && client.getConnectionInfo().localPort)} consumer local port: ${inspect(consumer && consumer.getConnectionInfo().localPort)} publisher local port: ${inspect(publisher && publisher.getConnectionInfo().localPort)}`)
  }, 3000)
}


/*
  at each interval, trigger a connection problem.
*/
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
      //after this message is logged, the client connections should reopen
      logger.info("\nNow it should reopen!\n")
    }, 60000)
  })
}

/*
  when setting up the publisher, we register a callback on the `publish_confirm` event that
  informs us that the broker has correctly received the sent message. This triggers an update on
  the outbox state (the message is considered as sent)
*/
async function setupPublisher(client) {
  const publisherRef = `publisher - ${randomUUID()}`
  const publisherConfig = { stream: streamName, publisherRef: publisherRef, connectionClosedListener: (err) => { return } }
  /*
    confirmedIds contains the list of publishingIds linked to messages correctly published in the stream
    These ids are not the messageIds that have been set in the message properties
  */
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

/*
  in the consumer we can use the `messageId` property to make sure each message is "handled" once.
 */
async function setupConsumer(client) {
  const consumerConfig = { stream: streamName, offset: rabbit.Offset.timestamp(new Date()), connectionClosedListener: (err) => { return } }
  const receiveCallback = (msg) => {
    const msgId = msg.messageProperties.messageId
    if (received.has(msgId)) {
      /*On restart, the consumer sets automatically its offset as the latest handled message index. 
        For sanity, some sort of deduplication is still needed.
        @see https://blog.rabbitmq.com/posts/2021/09/rabbitmq-streams-offset-tracking/
        and Consumer.storeOffset and Consumer.queryOffset for a more complete approach
      */
      logger.info(`dedup: ${msgId}`)
    }
    received.add(msgId)
    consumerOffset = msg.offset
    return
  }
  return client.declareConsumer(consumerConfig, receiveCallback)
}


/*
  setup of a client instance, a producer and a consumer. 
  The core of the example is represented by the implementation of the 
  `connection_closed` callback, in which the `client.restart()` method is called. 
  This triggers the reset of all TCP sockets involved, for all producers and consumers,
   as well as for the TCP socket used by the client itself.
*/
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
  //instantiate the client, the producer and the consumer
  await setup()
  //schedule the task that inserts new messages in the outbox
  scheduleMessageProduction()
  //schedule the task that attempts to send a message to the broker, taking it from the outbox
  await scheduleMessageDelivery()
  //schedule the task that logs connection info and message counters
  scheduleLogInfo()
  //schedule the task that triggers a (more or less simulated) network issue
  await triggerConnectionIssues()
}

main()
  .then(() => logger.info("done!"))
  .catch((res) => {
    logger.info("ERROR ", res)
    process.exit(-1)
  })

