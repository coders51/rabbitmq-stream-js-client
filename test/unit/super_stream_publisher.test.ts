import { expect } from "chai"
import { KeyValueCache } from "../../src/cache"
import { Client } from "../../src/client"
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

  clear(): void {
    this.entries.clear()
  }
}

describe("SuperStreamPublisher routing cache", () => {
  let routeQueries: string[]

  const stubLocator = () => {
    routeQueries = []
    const publisher = { send: async () => ({ sent: true }) as SendResult, close: async () => undefined }
    return {
      queryPartitions: async () => ["p0", "p1"],
      routeQuery: async ({ routingKey }: { routingKey: string }) => {
        routeQueries.push(routingKey)
        return ["p1"]
      },
      declarePublisher: async () => publisher as unknown as Publisher,
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

    expect(cache.gets).to.deep.equal(["k", "k"])
    expect(cache.sets).to.deep.equal([["k", "p1"]])
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
      routingCacheSize: -1,
      routingCache: cache,
    })

    await publisher.send(Buffer.from("m1"), { routingKey: "k" })
    await publisher.send(Buffer.from("m2"), { routingKey: "k" })

    expect(routeQueries).to.deep.equal(["k"])
  })

  it("does not touch the cache with the hash routing strategy", async () => {
    const cache = new RecordingCache()
    const publisher = await SuperStreamPublisher.create({
      locator: stubLocator(),
      superStream: "super",
      routingStrategy: "hash",
      routingCache: cache,
    })

    await publisher.send(Buffer.from("m1"), { routingKey: "k" })

    expect(cache.gets).to.deep.equal([])
    expect(routeQueries).to.deep.equal([])
  })
})
