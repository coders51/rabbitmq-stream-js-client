# RabbitMQ client for the stream protocol for Node.JS

# NOT READY FOR PRODUCTION - The client is HEAVILY work in progress.

[![Build Status](https://github.com/coders51/rabbitmq-stream-js-client/actions/workflows/main.yml/badge.svg)](https://github.com/coders51/rabbitmq-stream-js-client/actions)

# Table of Contents

- [Overview](#overview)
- [Installing via NPM](#installing-via-npm)
- [Getting started](#getting-started)
  - [Usage](#usage)
    - [Connect](#connect)
- [Build from source](#build-from-source)
- [Project Status](#project-status)
- [Release Process](#release-process)

## Getting started

A rapid getting started

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

## Build from source

Build:

```shell
$ npm run build
```

Test:

```shell
$ docker-compose up -d
$ npm run test
```

Check everything:

```shell
$ npm run check
```

## Project Status

The client is HEAVILY work in progress. The API(s) could change prior to version `1.0.0`

## MISC

https://github.com/rabbitmq/rabbitmq-server/blob/master/deps/rabbitmq_stream/docs/PROTOCOL.adoc
