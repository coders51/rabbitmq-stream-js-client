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

  it("floors a fractional maxSize", () => {
    const cache = new LruCache<number, number>(2.9)

    for (let i = 0; i < 10; i++) cache.set(i, i)

    expect(cache.size).to.equal(2)
  })

  it("is unbounded when maxSize is Infinity", () => {
    const cache = new LruCache<number, number>(Infinity)

    for (let i = 0; i < 1000; i++) cache.set(i, i)

    expect(cache.size).to.equal(1000)
    expect(cache.get(0)).to.equal(0)
  })

  it("disables caching when maxSize is 0 or a fraction below 1", () => {
    for (const maxSize of [0, 0.5]) {
      const cache = new LruCache<string, string>(maxSize)

      cache.set("a", "1")

      expect(cache.size, `maxSize ${maxSize}`).to.equal(0)
      expect(cache.get("a"), `maxSize ${maxSize}`).to.equal(undefined)
    }
  })

  it("throws when maxSize is negative or NaN", () => {
    for (const maxSize of [-1, -0.5, -Infinity, NaN]) {
      expect(() => new LruCache<string, string>(maxSize), `maxSize ${maxSize}`).to.throw(RangeError)
    }
  })
})
