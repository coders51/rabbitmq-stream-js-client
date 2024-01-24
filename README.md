# RabbitMQ client for the stream protocol for Node.JS

[![Build Status](https://github.com/coders51/rabbitmq-stream-js-client/actions/workflows/main.yml/badge.svg)](https://github.com/coders51/rabbitmq-stream-js-client/actions)

## Table of Contents

- [RabbitMQ client for the stream protocol for Node.JS](#rabbitmq-client-for-the-stream-protocol-for-nodejs)
  - [Overview](#overview)
  - [Installing via NPM](#installing-via-npm)
  - [Getting started](#getting-started)
  - [Usage](#usage)
    - [Connect](#connect)
    - [Connect through TLS/SSL](#connect-through-tls-ssl)
    - [Basic Publish](#basic-publish)
    - [Sub Batch Entry Publishing](#sub-batch-entry-publishing)
    - [Basic Consuming](#basic-consuming)
    - [Single Active Consumer](#single-active-consumer)
    - [Clustering](#clustering)
    - [Load Balancer](#loadbalancer)
  - [Running Examples](#running-examples)
  - [Build from source](#build-from-source)
  - [MISC](#misc)
  - [Super Stream](#super-stream)
  - [Filtering](#filtering)

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

Every time we create a new producer or a new consumer, a new connection is created.
In particular for the producer the connection is created on the node leader.
For more running the tests in a cluster follow the readme under the folder /cluster

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

Work in progress ⚠️

### Filtering

Work in progress ⚠️

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
