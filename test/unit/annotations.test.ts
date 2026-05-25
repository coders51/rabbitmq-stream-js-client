import { expect } from "chai"
import { Annotations } from "../../src/amqp10/messageAnnotations"
import { FormatCode } from "../../src/amqp10/decoder"
import { BufferDataReader, decodeFormatCode } from "../../src/response_decoder"

describe("Message annotations", () => {
  it("messageAnnotations with AMQP long encodings are read correctly", () => {
    const key = "x-dotnet-pub-seq-no"
    const smallLongAnnotations = Annotations.parse(
      new BufferDataReader(Buffer.concat([encodeAmqpString(key), Buffer.from([FormatCode.SmallLong, 0x7f])])),
      2
    )

    const longValue = Buffer.alloc(8)
    longValue.writeBigInt64BE(128n)
    const longAnnotations = Annotations.parse(
      new BufferDataReader(Buffer.concat([encodeAmqpString(key), Buffer.from([FormatCode.Long]), longValue])),
      2
    )

    expect(smallLongAnnotations).to.eql({ [key]: 127 })
    expect(longAnnotations[key] as unknown).to.eql(128n)
    expect(decodeFormatCode(new BufferDataReader(Buffer.from([0x7f])), FormatCode.SmallLong)).to.eql(
      smallLongAnnotations[key]
    )
  }).timeout(10000)
})

function encodeAmqpString(value: string) {
  const data = Buffer.from(value)
  return Buffer.concat([Buffer.from([FormatCode.Str8, data.length]), data])
}
