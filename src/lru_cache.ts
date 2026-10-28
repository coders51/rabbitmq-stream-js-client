export class LruCache<K, V> {
  private entries: Map<K, V> = new Map()

  constructor(private readonly maxSize: number) {
    if (!Number.isInteger(maxSize) || maxSize < 1) {
      throw new Error(`LruCache maxSize must be a positive integer, got ${maxSize}`)
    }
  }

  get size(): number {
    return this.entries.size
  }

  get(key: K): V | undefined {
    const value = this.entries.get(key)
    if (value === undefined) return undefined
    // Map iterates in insertion order: re-insert to mark as most recently used
    this.entries.delete(key)
    this.entries.set(key, value)
    return value
  }

  set(key: K, value: V): void {
    this.entries.delete(key)
    this.entries.set(key, value)
    if (this.entries.size > this.maxSize) {
      const oldest = this.entries.keys().next()
      if (!oldest.done) this.entries.delete(oldest.value)
    }
  }

  clear(): void {
    this.entries.clear()
  }
}
