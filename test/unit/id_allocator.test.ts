import { expect } from "chai"

import { IdAllocator } from "../../src/id_allocator"

describe("IdAllocator", () => {
  it("allocates unique ids", () => {
    const allocator = new IdAllocator()

    const ids = Array.from({ length: 256 }, () => allocator.allocate())

    expect(new Set(ids).size).to.equal(256)
  })

  it("throws when all ids are exhausted", () => {
    const allocator = new IdAllocator()
    for (let i = 0; i < 256; i++) allocator.allocate()

    expect(() => allocator.allocate()).to.throw("No more ids available on this connection")
  })

  it("makes a freed id available again", () => {
    const allocator = new IdAllocator()
    const id = allocator.allocate()
    allocator.free(id)

    const ids = Array.from({ length: 256 }, () => allocator.allocate())
    expect(ids).to.include(id)
  })

  it("reuses the freed id last (FIFO)", () => {
    const allocator = new IdAllocator()
    const id = allocator.allocate()
    allocator.free(id)

    const remaining = Array.from({ length: 256 }, () => allocator.allocate())
    expect(remaining.at(-1)).to.equal(id)
  })
})
