/*
    Run this example only with rabbit management version >= 3.13.0.

    Demonstrates how to tune or replace the cache the super stream publisher uses to remember
    routingKey -> partition lookups. The cache is only used with the "key" routingStrategy:
    the "hash" strategy computes the partition locally and never uses the cache.

    - routingCacheSize: size of the built-in LRU cache (default 10000, 0 = disabled, Infinity = unbounded, negative or NaN = error)
    - routingCache: your own cache object (get/set), it replaces the built-in one.
      Entries are keyed by "<superStream>:<routingKey>", so one cache can be shared by several super streams
*/

const rabbit = require("rabbitmq-stream-js-client")
const { randomUUID } = require("crypto")

const rabbitUser = process.env.RABBITMQ_USER || "rabbit"
const rabbitPassword = process.env.RABBITMQ_PASSWORD || "rabbit"

// A custom cache only needs get and set (see the exported SimpleStringCache interface).
// This one counts hits and misses so we can see how often the server needs to be queried.
// In a real application you could back it with a library such as lru-cache (to add a ttl, for example).
class CountingCache {
  constructor() {
    this.entries = new Map()
    this.hits = 0
    this.misses = 0
  }

  get(key) {
    const value = this.entries.get(key)
    if (value === undefined) this.misses++
    else this.hits++
    return value
  }

  set(key, partition) {
    this.entries.set(key, partition)
  }
}

async function main() {
  const superStreamName = `example-${randomUUID()}`
  console.log(`Creating super stream ${superStreamName}`)

  const client = await rabbit.connect({
    hostname: "localhost",
    port: 5552,
    username: rabbitUser,
    password: rabbitPassword,
    vhost: "/",
    heartbeat: 0,
  })
  await client.createSuperStream({ streamName: superStreamName })
  await sleep(200) // Waiting for partitions to be created

  // 1. Built-in LRU cache, sized with routingCacheSize:
  //    keeps the partitions of the 100 most recently used routing keys
  const boundedPublisher = await client.declareSuperStreamPublisher({
    superStream: superStreamName,
    routingStrategy: "key",
    routingCacheSize: 100,
  })
  await boundedPublisher.send(Buffer.from("bounded cache message"), { routingKey: "0" })

  // 2. Your own cache, passed with routingCache (routingCacheSize is ignored when it is provided)
  const cache = new CountingCache()
  const customPublisher = await client.declareSuperStreamPublisher({
    superStream: superStreamName,
    routingStrategy: "key",
    routingCache: cache,
  })

  // the routing keys of a default super stream are the partition indexes: "0", "1" and "2"
  for (const routingKey of ["0", "1", "0", "1", "0", "2"]) {
    await customPublisher.send(Buffer.from(`custom cache message for key ${routingKey}`), { routingKey })
  }

  // only the first message of each routing key had to ask the server
  console.log(`custom cache: ${cache.misses} misses (server queried), ${cache.hits} hits`)
  console.log(`cached keys: ${[...cache.entries.keys()].join(", ")}`)

  // sample output:
  /*
Creating super stream example-uuid
custom cache: 3 misses (server queried), 3 hits
cached keys: example-uuid:0, example-uuid:1, example-uuid:2
done!
*/

  await client.close()
}

main()
  .then(() => console.log("done!"))
  .catch((res) => {
    console.log("ERROR ", res)
    process.exit(-1)
  })
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
