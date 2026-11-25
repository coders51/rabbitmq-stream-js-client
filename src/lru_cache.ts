import { KeyValueCache } from "./cache"

export class LruCache<K, V> implements KeyValueCache<K, V> {
  private entries: Map<K, V> = new Map()
  private readonly maxSize: number

  /**
   * Creates a new LRU cache with the specified maximum size.
   * Fractional maxSize values are floored.
   * @param maxSize When 0 (or a fraction below 1), the cache is disabled. When Infinity, the cache is unbounded.
   * @throws RangeError when maxSize is negative or NaN
   */
  constructor(maxSize: number) {
    if (!(maxSize >= 0)) {
      throw new RangeError(`LruCache maxSize must be a number >= 0 (or Infinity), received ${maxSize}`)
    }
    this.maxSize = Math.floor(maxSize)
  }

  get size(): number {
    return this.entries.size
  }

  get(key: K): V | undefined {
    const value = this.entries.get(key)
    if (value === undefined) return undefined
    if (this.maxSize === Infinity) return value // unbounded: no eviction, so no need to track recency
    // Map iterates in insertion order: re-insert to mark as most recently used
    this.entries.delete(key)
    this.entries.set(key, value)
    return value
  }

  set(key: K, value: V): void {
    if (this.maxSize === 0) return // disabled
    this.entries.delete(key)
    this.entries.set(key, value)
    if (this.entries.size > this.maxSize) {
      const oldest = this.entries.keys().next()
      if (!oldest.done) this.entries.delete(oldest.value)
    }
  }
}
