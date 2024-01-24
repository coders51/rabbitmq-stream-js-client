import { expect } from "chai"
import { murmur32 } from "../../src/hash/murmur32"

describe("Murmur32x86 hashing algorithm", () => {
  it("the hashing function should produce results coherent with the implementation on other clients", () => {
    expect(murmur32("rabbit")).to.be.eql(3591948756)
    expect(murmur32("coders51")).to.be.eql(1856831182)
    expect(murmur32("d4c39ae6-2fc3-41a2-8771-161251e57d1a")).to.be.eql(1772425613)
  })
})
