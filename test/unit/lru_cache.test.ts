import { expect } from "chai"

import { LruCache } from "../../src/lru_cache"

describe("LruCache", () => {
  it("stores and retrieves values", () => {
    const cache = new LruCache<string, string>(2)

    cache.set("a", "1")

    expect(cache.get("a")).to.equal("1")
    expect(cache.get("missing")).to.equal(undefined)
  })

  it("never grows beyond maxSize", () => {
    const cache = new LruCache<number, number>(3)

    for (let i = 0; i < 100; i++) cache.set(i, i)

    expect(cache.size).to.equal(3)
  })

  it("evicts the least recently used entry", () => {
    const cache = new LruCache<string, string>(2)
    cache.set("a", "1")
    cache.set("b", "2")

    cache.get("a")
    cache.set("c", "3")

    expect(cache.get("a")).to.equal("1")
    expect(cache.get("b")).to.equal(undefined)
    expect(cache.get("c")).to.equal("3")
  })

  it("overwriting a key refreshes it without growing", () => {
    const cache = new LruCache<string, string>(2)
    cache.set("a", "1")
    cache.set("b", "2")

    cache.set("a", "updated")
    cache.set("c", "3")

    expect(cache.size).to.equal(2)
    expect(cache.get("a")).to.equal("updated")
    expect(cache.get("b")).to.equal(undefined)
  })

  it("rejects an invalid maxSize", () => {
    expect(() => new LruCache(0)).to.throw("maxSize")
    expect(() => new LruCache(1.5)).to.throw("maxSize")
  })
})
