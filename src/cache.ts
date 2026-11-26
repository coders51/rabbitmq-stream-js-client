/**
 * Minimal key/value cache contract. Implement it to plug a custom cache
 * (e.g. with TTLs or shared storage) into the library.
 */
export interface SimpleStringCache {
  /** Returns the cached value, or undefined on a miss */
  get(key: string): string | undefined
  /** Stores a value. Implementations may evict other entries or ignore the call */
  set(key: string, value: string): void
}
