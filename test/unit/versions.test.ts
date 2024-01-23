import { expect } from "chai"
import { checkServerDeclaredVersions, getClientSupportedVersions } from "../../src/versions"
import { NullLogger } from "../../src/logger"

describe("Versions", () => {
  const serverVersion = "3.13.0-rc.4"
  const logger = new NullLogger()
  it("client-side version declaration", () => {
    expect(getClientSupportedVersions(serverVersion).sort()).eql([
      { key: 22, maxVersion: 1, minVersion: 1 },
      { key: 13, maxVersion: 1, minVersion: 1 },
      { key: 29, maxVersion: 1, minVersion: 1 },
      { key: 9, maxVersion: 1, minVersion: 1 },
      { key: 1, maxVersion: 1, minVersion: 1 },
      { key: 6, maxVersion: 1, minVersion: 1 },
      { key: 14, maxVersion: 1, minVersion: 1 },
      { key: 30, maxVersion: 1, minVersion: 1 },
      { key: 27, maxVersion: 1, minVersion: 1 },
      { key: 23, maxVersion: 1, minVersion: 1 },
      { key: 15, maxVersion: 1, minVersion: 1 },
      { key: 16, maxVersion: 1, minVersion: 1 },
      { key: 21, maxVersion: 1, minVersion: 1 },
      { key: 17, maxVersion: 1, minVersion: 1 },
      { key: 2, maxVersion: 2, minVersion: 1 },
      { key: 11, maxVersion: 1, minVersion: 1 },
      { key: 5, maxVersion: 1, minVersion: 1 },
      { key: 19, maxVersion: 1, minVersion: 1 },
      { key: 18, maxVersion: 1, minVersion: 1 },
      { key: 10, maxVersion: 1, minVersion: 1 },
      { key: 28, maxVersion: 1, minVersion: 1 },
      { key: 7, maxVersion: 1, minVersion: 1 },
      { key: 20, maxVersion: 1, minVersion: 1 },
      { key: 12, maxVersion: 1, minVersion: 1 },
      { key: 24, maxVersion: 1, minVersion: 1 },
      { key: 25, maxVersion: 1, minVersion: 1 },
      { key: 8, maxVersion: 2, minVersion: 1 },
      { key: 3, maxVersion: 1, minVersion: 1 },
      { key: 4, maxVersion: 1, minVersion: 1 },
      { key: 26, maxVersion: 1, minVersion: 1 },
    ])
  })

  it("client-side version declaration with an older version of the server", () => {
    expect(getClientSupportedVersions("3.12.12").sort()).eql([
      { key: 22, maxVersion: 1, minVersion: 1 },
      { key: 13, maxVersion: 1, minVersion: 1 },
      { key: 9, maxVersion: 1, minVersion: 1 },
      { key: 1, maxVersion: 1, minVersion: 1 },
      { key: 6, maxVersion: 1, minVersion: 1 },
      { key: 14, maxVersion: 1, minVersion: 1 },
      { key: 27, maxVersion: 1, minVersion: 1 },
      { key: 23, maxVersion: 1, minVersion: 1 },
      { key: 15, maxVersion: 1, minVersion: 1 },
      { key: 16, maxVersion: 1, minVersion: 1 },
      { key: 21, maxVersion: 1, minVersion: 1 },
      { key: 17, maxVersion: 1, minVersion: 1 },
      { key: 2, maxVersion: 1, minVersion: 1 },
      { key: 11, maxVersion: 1, minVersion: 1 },
      { key: 5, maxVersion: 1, minVersion: 1 },
      { key: 19, maxVersion: 1, minVersion: 1 },
      { key: 18, maxVersion: 1, minVersion: 1 },
      { key: 10, maxVersion: 1, minVersion: 1 },
      { key: 28, maxVersion: 1, minVersion: 1 },
      { key: 7, maxVersion: 1, minVersion: 1 },
      { key: 20, maxVersion: 1, minVersion: 1 },
      { key: 12, maxVersion: 1, minVersion: 1 },
      { key: 24, maxVersion: 1, minVersion: 1 },
      { key: 25, maxVersion: 1, minVersion: 1 },
      { key: 8, maxVersion: 1, minVersion: 1 },
      { key: 3, maxVersion: 1, minVersion: 1 },
      { key: 4, maxVersion: 1, minVersion: 1 },
      { key: 26, maxVersion: 1, minVersion: 1 },
    ])
  })

  it("compare versions, server-side all defaults, ok", () => {
    expect(checkServerDeclaredVersions([], logger)).to.eql(true)
  })

  it("compare versions, server-side specifies greater max version, ok", () => {
    expect(checkServerDeclaredVersions([{ key: 20, maxVersion: 99, minVersion: 1 }], logger)).to.eql(true)
  })

  it("compare versions, server-side specifies message, fallback on client side, ok", () => {
    expect(checkServerDeclaredVersions([{ key: -99, maxVersion: 1, minVersion: 1 }], logger)).to.eql(true)
  })

  it("compare versions, server-side specifies greater min version, ko", () => {
    expect(checkServerDeclaredVersions([{ key: 20, maxVersion: 10, minVersion: 2 }], logger)).to.eql(false)
  })

  it("compare versions, server-side specifies smaller max version, ko", () => {
    expect(checkServerDeclaredVersions([{ key: 20, maxVersion: -1, minVersion: -2 }], logger)).to.eql(false)
  })
})
