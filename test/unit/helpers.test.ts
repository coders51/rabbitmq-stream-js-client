import { expect } from "chai"
import { Client, Offset, resolveStartOffset } from "../../src"
import { StreamResponseError } from "../../src/stream_response_error"
import { ResponseCode } from "../../src/util"
import { expectToThrowAsync } from "../support/util"

describe("resolveStartOffset", () => {
  const fakeClient = (queryOffset: (params: { reference: string; stream: string }) => Promise<bigint>) =>
    ({ queryOffset }) as unknown as Client

  it("returns the offset following the stored one when the server has a stored offset", async () => {
    const client = fakeClient(async () => 41n)

    const offset = await resolveStartOffset(client, Offset.first(), "ref", "stream")

    expect(offset).to.eql(Offset.offset(42n))
  })

  it("returns offset 1 when the stored offset is 0", async () => {
    const client = fakeClient(async () => 0n)

    const offset = await resolveStartOffset(client, Offset.first(), "ref", "stream")

    expect(offset).to.eql(Offset.offset(1n))
  })

  it("returns the default offset when no offset is stored", async () => {
    const client = fakeClient(async () => {
      throw new StreamResponseError("No offset", ResponseCode.NoOffset)
    })

    const offset = await resolveStartOffset(client, Offset.next(), "ref", "stream")

    expect(offset).to.eql(Offset.next())
  })

  it("queries the server with the given consumer reference and stream", async () => {
    const calls: { reference: string; stream: string }[] = []
    const client = fakeClient(async (params) => {
      calls.push(params)
      return 1n
    })

    await resolveStartOffset(client, Offset.first(), "my-ref", "my-stream")

    expect(calls).to.eql([{ reference: "my-ref", stream: "my-stream" }])
  })

  it("rethrows stream response errors other than NoOffset", async () => {
    const client = fakeClient(async () => {
      throw new StreamResponseError("Stream does not exist", ResponseCode.StreamDoesNotExist)
    })

    await expectToThrowAsync(
      () => resolveStartOffset(client, Offset.first(), "ref", "stream"),
      StreamResponseError,
      /Stream does not exist/
    )
  })

  it("rethrows errors that are not stream response errors", async () => {
    const client = fakeClient(async () => {
      throw new Error("connection lost")
    })

    await expectToThrowAsync(
      () => resolveStartOffset(client, Offset.first(), "ref", "stream"),
      Error,
      /connection lost/
    )
  })
})
