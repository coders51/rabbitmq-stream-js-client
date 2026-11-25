import { expect } from "chai"
import { KeyValueCache } from "../../src/cache"
import { Client } from "../../src/client"
import { murmur32 } from "../../src/hash/murmur32"
import { Publisher, SendResult } from "../../src/publisher"
import { SuperStreamPublisher } from "../../src/super_stream_publisher"

class RecordingCache implements KeyValueCache<string, string> {
  readonly entries = new Map<string, string>()
  readonly gets: string[] = []
  readonly sets: [string, string][] = []

  get(key: string): string | undefined {
    this.gets.push(key)
    return this.entries.get(key)
  }

  set(key: string, value: string): void {
    this.sets.push([key, value])
    this.entries.set(key, value)
  }
}

describe("SuperStreamPublisher routing cache", () => {
  let routeQueries: string[]
  let declaredStreams: string[]

  const stubLocator = () => {
    routeQueries = []
    declaredStreams = []
    const publisher = { send: async () => ({ sent: true }) as SendResult, close: async () => undefined }
    return {
      queryPartitions: async () => ["p0", "p1"],
      routeQuery: async ({ routingKey }: { routingKey: string }) => {
        routeQueries.push(routingKey)
        return ["p1"]
      },
      declarePublisher: async ({ stream }: { stream: string }) => {
        declaredStreams.push(stream)
        return publisher as unknown as Publisher
      },
    } as unknown as Client
  }

  it("uses a custom routingCache with the key routing strategy", async () => {
    const cache = new RecordingCache()
    const publisher = await SuperStreamPublisher.create({
      locator: stubLocator(),
      superStream: "super",
      routingStrategy: "key",
      routingCache: cache,
    })

    await publisher.send(Buffer.from("m1"), { routingKey: "k" })
    await publisher.send(Buffer.from("m2"), { routingKey: "k" })

    expect(cache.gets).to.deep.equal(["super:k", "super:k"])
    expect(cache.sets).to.deep.equal([["super:k", "p1"]])
    expect(routeQueries).to.deep.equal(["k"])
  })

  it("queries the server on every send when the custom cache never stores", async () => {
    const noopCache: KeyValueCache<string, string> = {
      get: () => undefined,
      set: () => undefined,
      clear: () => undefined,
    }
    const publisher = await SuperStreamPublisher.create({
      locator: stubLocator(),
      superStream: "super",
      routingStrategy: "key",
      routingCache: noopCache,
    })

    await publisher.send(Buffer.from("m1"), { routingKey: "k" })
    await publisher.send(Buffer.from("m2"), { routingKey: "k" })

    expect(routeQueries).to.deep.equal(["k", "k"])
  })

  it("routingCache takes precedence over routingCacheSize", async () => {
    const cache = new RecordingCache()
    const publisher = await SuperStreamPublisher.create({
      locator: stubLocator(),
      superStream: "super",
      routingStrategy: "key",
      routingCacheSize: 0,
      routingCache: cache,
    })

    await publisher.send(Buffer.from("m1"), { routingKey: "k" })
    await publisher.send(Buffer.from("m2"), { routingKey: "k" })

    expect(routeQueries).to.deep.equal(["k"])
  })

  describe("built-in cache sized with routingCacheSize", () => {
    const sendKeys = async (keys: string[], routingCacheSize?: number) => {
      const publisher = await SuperStreamPublisher.create({
        locator: stubLocator(),
        superStream: "super",
        routingStrategy: "key",
        routingCacheSize,
      })
      for (const routingKey of keys) {
        await publisher.send(Buffer.from("m"), { routingKey })
      }
    }

    it("caches routing keys by default", async () => {
      await sendKeys(["k", "k", "k"])

      expect(routeQueries).to.deep.equal(["k"])
    })

    it("evicts the least recently used key when full", async () => {
      await sendKeys(["a", "b", "a"], 1)

      expect(routeQueries).to.deep.equal(["a", "b", "a"])
    })

    it("keeps up to routingCacheSize keys", async () => {
      await sendKeys(["a", "b", "a", "b"], 2)

      expect(routeQueries).to.deep.equal(["a", "b"])
    })

    it("is unbounded when routingCacheSize is Infinity", async () => {
      const keys = Array.from({ length: 50 }, (_, i) => `k${i}`)

      await sendKeys([...keys, ...keys], Infinity)

      expect(routeQueries).to.deep.equal(keys)
    })

    it("disables the cache when routingCacheSize is 0", async () => {
      await sendKeys(["k", "k", "k"], 0)

      expect(routeQueries).to.deep.equal(["k", "k", "k"])
    })

    it("throws when routingCacheSize is negative or NaN", async () => {
      for (const size of [-1, NaN]) {
        let error: unknown
        try {
          await sendKeys(["k"], size)
        } catch (e) {
          error = e
        }
        expect(error, `routingCacheSize ${size}`).to.be.instanceOf(RangeError)
      }
    })
  })

  it("does not use the cache with the hash routing strategy", async () => {
    const routingKey = "k"
    const partitions = ["p0", "p1"]
    const hashPartition = partitions[murmur32(routingKey) % partitions.length]
    const poisonedPartition = partitions.find((p) => p !== hashPartition) as string
    // a cache that would send the key somewhere else if it were consulted
    const cache = new RecordingCache()
    cache.entries.set(`super:${routingKey}`, poisonedPartition)
    const publisher = await SuperStreamPublisher.create({
      locator: stubLocator(),
      superStream: "super",
      routingStrategy: "hash",
      routingCache: cache,
    })

    await publisher.send(Buffer.from("m1"), { routingKey })
    await publisher.send(Buffer.from("m2"), { routingKey })

    expect(declaredStreams).to.deep.equal([hashPartition])
    expect(cache.gets).to.deep.equal([])
    expect(cache.sets).to.deep.equal([])
    expect(routeQueries).to.deep.equal([])
  })
})
