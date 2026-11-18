/**
 * Minimal key/value cache contract. Implement it to plug a custom cache
 * (e.g. with TTLs or shared storage) into the library.
 */
export interface KeyValueCache<K, V> {
  /** Returns the cached value, or undefined on a miss */
  get(key: K): V | undefined
  /** Stores a value. Implementations may evict other entries or ignore the call */
  set(key: K, value: V): void
  /** Removes all entries */
  clear(): void
}
