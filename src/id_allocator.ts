export class IdAllocator {
  private freeIds: number[]

  constructor() {
    this.freeIds = [...Array(256).keys()]
  }

  public allocate(): number {
    const id = this.freeIds.shift()

    if (id === undefined) {
      throw new Error("No more ids available on this connection")
    }

    return id
  }

  public free(id: number) {
    this.freeIds.push(id)
  }
}
