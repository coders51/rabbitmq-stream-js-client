/*

  Super stream auto-reconnect example: restart only the connections that closed

  Run this example only with rabbit management version >= 3.13.0.

  Every partition of a super stream gets its own publisher/consumer connection, so a network problem
  usually affects only some of them. The `connectionClosedListener` receives the id of the connection
  that closed, and `client.restart([connectionId])` restarts just that connection (and the publishers
  and consumers living on it), leaving the healthy ones untouched.

  In this example we assume that:
    - the super stream topology does not change (i.e. leader/replicas nodes do not change)
    - hostnames/ip addresses do not change

  The example is composed of
    - a client instance with a `connection_closed` listener for the locator connection and a
      `publish_confirm` listener (the listeners passed to `connect()` are applied to every partition connection too,
      and receive the id of the connection they fire on)
    - a super stream publisher and a super stream consumer, each with a `connectionClosedListener`
      that restarts only the closed connection
    - toy outbox pattern implementation (in-memory structure, no persistence of data, etc.)
    - scheduled delivery of messages through the publisher, with publish confirm handling
    - a scheduled reachability interruption (in this case obtained by launching `docker-compose restart`)
    - a scheduled process that logs the state of the application (message counters, restarts)
*/

const rabbit = require("rabbitmq-stream-js-client")
const { randomUUID } = require("crypto")
const { exec } = require("child_process")
const { promisify, inspect } = require("util")
const promiseExec = promisify(exec)

const rabbitUser = process.env.RABBITMQ_USER || "rabbit"
const rabbitPassword = process.env.RABBITMQ_PASSWORD || "rabbit"
let client = undefined
let publisher = undefined
let consumer = undefined
const superStreamName = `example-${randomUUID()}`
const received = new Set()
const restarting = new Set()
/*
  outbox: messages waiting to be sent, and messages sent but not yet confirmed.
  Publishing ids are per partition publisher, so a sent message is identified by connection id + publisher id + publishing id.
*/
const outbox = { queue: [], pending: new Map() }
const pendingKey = (connectionId, publisherId, publishingId) => `${connectionId}/${publisherId}/${publishingId}`
let restartCount = 0
let published = 0
let confirmed = 0
let sendErrors = 0
let callbackCalled = 0
const logger = {
  debug: (msg) => {
    return
  },
  info: (msg) => console.log(`[info]\t[${new Date().toISOString()}] ${msg}`),
  error: (msg) => console.log(`[error]\t[${new Date().toISOString()}] ${msg}`),
  warn: (msg) => console.log(`[warn]\t[${new Date().toISOString()}] ${msg}`),
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

function getNodesFromEnv() {
  const envValue = process.env.RABBIT_MQ_TEST_NODES ?? "localhost:5552"
  const nodes = envValue.split(";")
  return nodes.map((n) => {
    const [host, port] = n.split(":")
    return { host: host ?? "localhost", port: parseInt(port) ?? 5552 }
  })
}

async function triggerConnectionIssue() {
  await promiseExec("cd .. && docker-compose restart")
  return true
}

/*
  Restart only the connection that closed.
  The listener can be invoked more than once for the same connection (e.g. once per consumer living on it),
  so concurrent restarts of the same connection id are skipped.
*/
function restartConnection(hadError, connectionId) {
  logger.info(`Connection ${connectionId} closed (hadError: ${hadError})`)
  callbackCalled++
  if (callbackCalled > 100) {
    process.exit(0)
  }
  if (restarting.has(connectionId)) return
  restarting.add(connectionId)
  requeueUnconfirmed(connectionId)
  client
    .restart([connectionId])
    .then(() => logger.info(`Connection ${connectionId} restarted!`))
    .catch((reason) => logger.warn(`Could not restart connection ${connectionId}: ${inspect(reason)}`))
    .finally(() => restarting.delete(connectionId))
}

/*
  messages sent on a connection that closed may never be confirmed: put them back in the queue to be sent again.
  The consumer deduplicates by messageId, so a message that was in fact stored is not handled twice.
*/
function requeueUnconfirmed(connectionId) {
  for (const [key, message] of outbox.pending) {
    if (message.connectionId !== connectionId) continue
    outbox.pending.delete(key)
    outbox.queue.push({ payload: message.payload, messageId: message.messageId })
  }
}

/*
  the `publish_confirm` listener registered in `connect()` informs us that the broker has correctly received
  the sent messages. The confirm carries the publisher id and the publishing ids, and the listener
  is also given the id of the connection the confirm arrived on.
*/
function publishConfirmCallback(confirm, connectionId) {
  for (const publishingId of confirm.publishingIds) {
    if (outbox.pending.delete(pendingKey(connectionId, confirm.publisherId, publishingId))) confirmed++
  }
}

/*
  very simple message generation function. The messageId is also used as routing key,
  so messages are spread across the partitions of the super stream.
*/
function generateMessage() {
  return { payload: Buffer.from(`${randomUUID()}`), messageId: `${randomUUID()}` }
}

/*
  at each iteration, a new message is put in the outbox. This mimicks a client application that generates messages to be sent.
*/
function scheduleMessageProduction() {
  setInterval(() => outbox.queue.push(generateMessage()), 50)
}

/*
  at each iteration, a message is taken from the outbox queue and sent using the super stream publisher.
  While a partition connection is being restarted the send may fail: the message is put back in the queue
  and will be retried at the next iteration. Once sent, the message stays in the pending map until it is confirmed.
*/
function scheduleMessageDelivery() {
  let sending = false
  setInterval(async () => {
    if (!publisher || sending || outbox.queue.length === 0) return
    sending = true
    const message = outbox.queue.shift()
    try {
      const res = await publisher.send(message.payload, { messageProperties: { messageId: message.messageId } })
      published++
      outbox.pending.set(pendingKey(res.connectionId, res.publisherId, res.publishingId), {
        ...message,
        connectionId: res.connectionId,
      })
    } catch (e) {
      sendErrors++
      outbox.queue.unshift(message)
    } finally {
      sending = false
    }
  }, 10)
}

function scheduleLogInfo() {
  setInterval(() => {
    logger.info(`outbox queued: ${outbox.queue.length} pending confirm: ${outbox.pending.size}`)
    logger.info(
      `${inspect({ published, confirmed, sendErrors, received: received.size, restarting: [...restarting] })}`
    )
    logger.info(`client local port: ${inspect(client && client.getConnectionInfo().localPort)}`)
  }, 3000)
}

/*
  at each interval, trigger a connection problem.
*/
function triggerConnectionIssues() {
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
        } catch (e) {
          rej(e)
          return
        }
      }
      //after this message is logged, the closed connections should reopen
      logger.info("\nNow it should reopen!\n")
    }, 60000)
  })
}

async function setup() {
  const firstNode = getNodesFromEnv()[0]
  logger.info(`Now invoking rabbit.connect on ${inspect(firstNode)}`)
  client = await rabbit.connect(
    {
      hostname: firstNode.host,
      port: firstNode.port,
      username: rabbitUser,
      password: rabbitPassword,
      // `connection_closed` here covers the locator connection only: partition publishers/consumers have their own connections
      listeners: {
        connection_closed: restartConnection,
        publish_confirm: publishConfirmCallback,
      },
      vhost: "/",
      heartbeat: 0,
    },
    logger
  )
  await client.createSuperStream({ streamName: superStreamName })
  await sleep(200) // Waiting for partitions to be created

  const routingKeyExtractor = (content, msgOptions) => msgOptions.messageProperties.messageId
  publisher = await client.declareSuperStreamPublisher(
    {
      superStream: superStreamName,
      publisherRef: `publisher - ${randomUUID()}`,
      connectionClosedListener: restartConnection,
    },
    routingKeyExtractor
  )

  /*
    in the consumer we can use the `messageId` property to make sure each message is "handled" once:
    on restart each consumer resumes from its last handled offset, but some deduplication is still advisable.
    @see https://blog.rabbitmq.com/posts/2021/09/rabbitmq-streams-offset-tracking/
  */
  consumer = await client.declareSuperStreamConsumer(
    {
      superStream: superStreamName,
      offset: rabbit.Offset.timestamp(new Date()),
      connectionClosedListener: restartConnection,
    },
    (msg) => {
      const msgId = msg.messageProperties.messageId
      if (received.has(msgId)) logger.info(`dedup: ${msgId}`)
      received.add(msgId)
    }
  )
}

async function main() {
  //instantiate the client, the super stream publisher and the super stream consumer
  await setup()
  //schedule the task that inserts new messages in the outbox
  scheduleMessageProduction()
  //schedule the task that attempts to send a message to the super stream, taking it from the outbox
  scheduleMessageDelivery()
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
