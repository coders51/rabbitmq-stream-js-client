# RabbitMQ client for the stream protocol for Node.JS

[![Build Status](https://github.com/coders51/rabbitmq-stream-js-client/actions/workflows/main.yml/badge.svg)](https://github.com/coders51/rabbitmq-stream-js-client/actions)

## Table of Contents

- [RabbitMQ client for the stream protocol for Node.JS](#rabbitmq-client-for-the-stream-protocol-for-nodejs)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Installing via NPM](#installing-via-npm)
  - [Getting started](#getting-started)
  - [Usage](#usage)
    - [Connect](#connect)
    - [Connect through TLS/SSL](#connect-through-tlsssl)
    - [Basic Publish](#basic-publish)
    - [Sub Batch Entry Publishing](#sub-batch-entry-publishing)
    - [Basic Consuming](#basic-consuming)
    - [Single Active Consumer](#single-active-consumer)
    - [Clustering](#clustering)
    - [Load Balancer](#load-balancer)
    - [Super Stream](#super-stream)
    - [Filtering](#filtering)
    - [Mitigating connection issues](#mitigating-connection-issues)
  - [Running Examples](#running-examples)
  - [Build from source](#build-from-source)
  - [MISC](#misc)

## Overview

A client for the RabbitMQ stream protocol, written (and ready for) Typescript

## Installing via NPM

```shell
npm install rabbitmq-stream-js-client
```

## Getting started

A quick getting started

```typescript
const rabbit = require("rabbitmq-stream-js-client")

async function main() {
  const client = await rabbit.connect({
    hostname: "localhost",
    port: 5552,
    username: "rabbit",
    password: "rabbit",
    vhost: "/",
  })

  await client.close()
}

main()
  .then(() => console.log("done!"))
  .catch((res) => console.log("ERROR ", res))
```

## Usage

---

### Connect

```typescript
const client = await connect({
  hostname: "localhost",
  port: 5552,
  username: "rabbit",
  password: "rabbit",
  vhost: "/",
})

// ...

await client.close()
```

### Connect through TLS/SSL

```typescript
const client = await connect({
  hostname: "localhost",
  port: 5552,
  username: "rabbit",
  password: "rabbit",
  vhost: "/",
  ssl: {
    key: "<client_private_key>",
    cert: "<client_certificate>",
    ca: "<ca>", // Optional
  },
})

// ...

await client.close()
```

### Basic Publish

```typescript
const client = await connect({
  hostname: "localhost",
  port: 5552,
  username: "rabbit",
  password: "rabbit",
  vhost: "/",
})

const publisher = await client.declarePublisher({
  stream: "stream-name",
  publisherRef: "my-publisher",
})

await publisher.send(Buffer.from("my message content"))

// ...

await client.close()
```

### Sub Batch Entry Publishing

```typescript
const client = await connect({
  hostname: "localhost",
  port: 5552,
  username: "rabbit",
  password: "rabbit",
  vhost: "/",
})

const publisher = await client.declarePublisher({
  stream: "stream-name",
  publisherRef: "my-publisher",
})

const messages = [
  { content: Buffer.from("my message content 1") },
  { content: Buffer.from("my message content 2") },
  { content: Buffer.from("my message content 3") },
  { content: Buffer.from("my message content 4") },
]

await publisher.sendSubEntries(messages)
/*
  It is also possible to specify a compression when sending sub entries of messages:
  e.g:  await publisher.sendSubEntries(messages, CompressionType.Gzip)

  The current values for the compression types are CompressionType.None or CompressionType.Gzip
*/

await client.close()
```

### Basic Consuming

```typescript
const client = await connect({
  hostname: "localhost",
  port: 5552,
  username: "rabbit",
  password: "rabbit",
  vhost: "/",
})

const consumerOptions = { stream: "stream-name", offset: Offset.next() }
/*
  When creating a consumer the offset and the stream name are mandatory parameters.
  The offset parameter can be created from one of the following functions:
    - Offset.first()        ---> Start reading from the first available offset.
    - Offset.next()         ---> Start reading from the next offset to be written.
    - Offset.last()         ---> Start reading from the last chunk of messages in the stream.
    - Offset.offset(x)      ---> Start reading from the specified offset. The parameter has to be a bigint.
    - Offset.timestamp(t)   ---> Start reading from the messages stored after the timestamp t.

*/

const consumer = await client.declareConsumer(consumerOptions, (message: Message) => {
  console.log(message.content) // it's a Buffer
})

// declareConsumer works even with sub batch entry publishing and compression

// ...

await client.close()
```

### Single Active Consumer

It is possible to create a consumer as single active.
For the given reference only one consumer will be able to consume messages

```typescript
const consumerOptions = {
  stream: "stream-name",
  offset: Offset.next(),
  singleActive: true,
  consumerRef: "my-consumer-ref",
} // see docs for various offset types

const consumer = await client.declareConsumer(consumerOptions, (message: Message) => {
  console.log(message.content) // it's a Buffer
})
// ...
```

### Clustering

Every time we create a new producer or a new consumer, a new connection object is created. The underlying TCP connections can be shared among different producers and different consumers. Note however that:

- each `Client` instance has a unique connection, which is not shared in any case.
- for producers the connection is created on the node leader.
- consumers and producers do not share connections.
  For more about running the tests in a cluster follow the readme under the folder /cluster

### Load Balancer

With the load balancer, what happens is we will connect first to the AddressResolver
and then we will connect to the node through the AddressResolver.
The address resolver is going to give us a node leader for a Producer and a node
replica for the consumer, otherwise it will close the connection and retry.

```typescript
const client = await connect({
  hostname: "node0",
  port: 5562,
  username: "rabbit",
  password: "rabbit",
  vhost: "/",
  addressResolver: { enabled: true },
})

const streamName = "my-stream"
await rabbit.createStream(streamName)

await wait(200) // wait for replicas to be created

// ...

await client.close()
```

### Super Stream

It is possible to create a super stream directly through the client only if you are using the latest (3.13.0-rc) management version.
Currently we do not support batch publishing and compression - that feature is coming soon

```typescript
const client = await rabbit.connect({
  hostname: "localhost",
  port: 5552,
  username: rabbitUser,
  password: rabbitPassword,
  vhost: "/",
  heartbeat: 0,
})
await client.createSuperStream({ streamName: "super-stream-example" })
await sleep(200) // Waiting for partitions to be created

const routingKeyExtractor = (content, msgOptions) => msgOptions.messageProperties.messageId
const publisher = await client.declareSuperStreamPublisher({ superStream: "super-stream-example" }, routingKeyExtractor)

await publisher.send(Buffer.from("Test message 1"), { messageProperties: { messageId: "1" } })
await publisher.send(Buffer.from("Test message 2"), { messageProperties: { messageId: "2" } })
await publisher.send(Buffer.from("Test message 3"), { messageProperties: { messageId: "3" } })

await client.declareSuperStreamConsumer({ superStream: "super-stream-example" }, (message) => {
  console.log(`Received message ${message.content.toString()}`)
})

await sleep(2000)

await client.close()
```

### Filtering

It is possible to tag messages while publishing and filter them on both the broker side and client side

```typescript
const client = await connect({
  hostname: "localhost",
  port: 5552,
  username: "rabbit",
  password: "rabbit",
  vhost: "/",
})

const publisher = await client.declarePublisher(
  { stream: streamName, publisherRef: `my-publisher-${randomUUID()}` },
  (msg) => msg.applicationProperties!["test"].toString() // Tags the message
)
const message1 = "test1"
const message2 = "test2"
const message3 = "test3"
const applicationProperties1 = { test: "A" }
const applicationProperties2 = { test: "B" }

await publisher.send(Buffer.from(message1), { applicationProperties: applicationProperties1 })
await publisher.send(Buffer.from(message2), { applicationProperties: applicationProperties1 })
await publisher.send(Buffer.from(message3), { applicationProperties: applicationProperties2 })

await client.declareConsumer(
  {
    stream: streamName,
    offset: Offset.first(),
    // Filter option for the consumer
    filter: {
      values: ["A", "B"],
      postFilterFunc: (msg) => msg.applicationProperties!["test"] === "A",
      matchUnfiltered: true,
    },
  },
  (msg) => filteredMsg.push(msg.content.toString("utf-8"))
)

await sleep(2000)

await client.close()
```

### Mitigating connection issues

The library exposes some utility functions and properties that can help in building a more robust client application. One simple use case that is addressed in one of the examples (`example/autoreconnect_example.js`) shows how to build a client application that can handle simple network issues like a temporary disconnection. In this scenario we are _not_ dealing with a complex broker-side service disruption or a cluster reorganization; in particular, we assume that the stream topology and the node host names do not change.

The approach can be simply summed up as: register a `connection_closed` listener when instantiating a `Client` object, and then call the `client.restart().then(...)` method in its body.

```typescript
const connectionClosedCallback = () => {
  logger.info(`In connection closed event...`)
  client
    .restart()
    .then(() => {
      logger.info(`Connections restarted!`)
    })
    .catch((reason) => {
      logger.warn(`Could not reconnect to Rabbit! ${reason}`)
    })
}

client = await rabbit.connect({
  //...
  listeners: { connection_closed: connectionClosedCallback },
  //...
})
```

There are various considerations to keep in mind when building a client application around these features:

- this `connection_closed` callback is registered on the event of the TCP socket used by the `Client` object instance. This callback cannot be an `async` function, so we need to use `then(...).catch(...)` to deal with the outcome of the restart attempt.
- clients, producers and consumers expose a utility function `getConnectionInfo()` that returns information about the state of the underlying TCP socket and the logical connection to the broker. In particular, the `ready` field indicates if the handshake with the broker completed correctly.
- consider using the outbox pattern when sending messages to the broker.
- some form of deduplication should be implementend in the client application when receiving messages: the use of offsets when defining consumer instances does not avoid the possibility of receiving messages multiple times.

See the comments and the implemenation in `example/autoreconnect_example.js` for a more in-depth view.

## Running Examples

the folder /example contains a project that shows some examples on how to use the lib, to run it follow this steps

move to the example folder and install the dependencies

```shell
cd example
npm i
```

run the docker-compose to launch a rabbit instance already stream enabled

```shell
docker-compose up -d
```

add this line to your host file (on linux `/etc/hosts`) to correctly resolve rabbitmq

```shell
127.0.0.1       rabbitmq
```

then launch the examples

```shell
npm start
```

## Build from source

Build:

```shell
npm run build
```

Test:

```shell
docker-compose up -d
npm run test
```

Check everything:

```shell
npm run check
```

## MISC

<https://github.com/rabbitmq/rabbitmq-server/blob/master/deps/rabbitmq_stream/docs/PROTOCOL.adoc>
