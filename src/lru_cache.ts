export class LruCache<K, V> {
  private entries: Map<K, V> = new Map()
  private readonly disabled: boolean

  /**
   * Creates a new LRU cache with the specified maximum size.
   * Fractional maxSize values are floored.
   * @param maxSize When 0, cache is unbounded. When <0 (or NaN) cache is disable.
   */
  constructor(private readonly maxSize: number) {
    this.disabled = !(maxSize >= 0)
    this.maxSize = this.disabled ? 0 : Math.floor(maxSize)
  }

  get size(): number {
    return this.entries.size
  }

  get(key: K): V | undefined {
    const value = this.entries.get(key)
    if (value === undefined) return undefined
    if (this.maxSize === 0) return value // unbounded: no eviction, so no need to track recency
    // Map iterates in insertion order: re-insert to mark as most recently used
    this.entries.delete(key)
    this.entries.set(key, value)
    return value
  }

  set(key: K, value: V): void {
    if (this.disabled) return
    this.entries.delete(key)
    this.entries.set(key, value)
    if (this.maxSize > 0 && this.entries.size > this.maxSize) {
      const oldest = this.entries.keys().next()
      if (!oldest.done) this.entries.delete(oldest.value)
    }
  }

  clear(): void {
    this.entries.clear()
  }
}
