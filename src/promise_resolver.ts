export interface PromiseResolver<T> {
  resolve: (value: T | PromiseLike<T>) => void
  reject: (reason: unknown) => void
}
