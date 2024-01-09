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
    - [Basic Publish](#basic-publish)
    - [Basic Consuming](#basic-consuming)
  - [Running Examples](#running-examples)
  - [Build from source](#build-from-source)
  - [Project Status](#project-status)
  - [MISC](#misc)

## Overview

NOT READY FOR PRODUCTION - The client is HEAVILY work in progress.

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

### Basic Publish

```typescript
const connection = await connect({
  hostname: "localhost",
  port: 5552,
  username: "rabbit",
  password: "rabbit",
  vhost: "/",
})

const publisher = await connection.declarePublisher({
  stream: "stream-name",
  publisherRef: "my-publisher",
})

await publisher.send(Buffer.from("my message content"))

// ...

await connection.close()
```

### Basic Consuming

```typescript
const connection = await connect({
  hostname: "localhost",
  port: 5552,
  username: "rabbit",
  password: "rabbit",
  vhost: "/",
})

const consumerOptions = { stream: "stream-name", offset: Offset.next() }  // see docs for various offset types

const consumer = await connection.declareConsumer(consumerOptions, (message: Message) => {
  console.log(message.content) // it's a Buffer 
})

// ...

await connection.close()
```

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

## Project Status

The client is HEAVILY work in progress. The API(s) could change prior to version `1.0.0`

## MISC

<https://github.com/rabbitmq/rabbitmq-server/blob/master/deps/rabbitmq_stream/docs/PROTOCOL.adoc>
