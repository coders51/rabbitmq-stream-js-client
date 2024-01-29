import { expect } from "chai"
import { NoneCompression } from "../../src/compression"
import { DecoderListenerFunc } from "../../src/decoder_listener"
import { ResponseDecoder } from "../../src/response_decoder"
import { PeerPropertiesResponse } from "../../src/responses/peer_properties_response"
import { Response } from "../../src/responses/response"
import { createConsoleLog } from "../support/util"
import { BufferDataWriter } from "../../src/requests/buffer_data_writer"

class MockDecoderListener {
  readonly responses: Response[] = []

  reset() {
    this.responses.splice(0)
  }

  responseReceived(data: Response) {
    this.responses.push(data)
  }

  buildListener(): DecoderListenerFunc {
    this.reset()
    return (...args) => this.responseReceived(...args)
  }
}

describe("ResponseDecoder", () => {
  let decoder: ResponseDecoder
  const mockListener = new MockDecoderListener()
  const getCompressionBy = () => NoneCompression.create()

  beforeEach(() => {
    decoder = new ResponseDecoder(mockListener.buildListener(), createConsoleLog())
  })

  it("decode a buffer that contains a single response", () => {
    const data = createResponse({ key: PeerPropertiesResponse.key })

    decoder.add(data, getCompressionBy)

    expect(mockListener.responses).lengthOf(1)
  })

  it("decode a buffer that contains multiple responses", () => {
    const data = [
      createResponse({ key: PeerPropertiesResponse.key }),
      createResponse({ key: PeerPropertiesResponse.key }),
    ]

    decoder.add(Buffer.concat(data), getCompressionBy)

    expect(mockListener.responses).lengthOf(2)
  })
})

function createResponse(params: { key: number; correlationId?: number; responseCode?: number }): Buffer {
  const bufferSize = 1024
  const bufferSizeParams = { maxSize: bufferSize }
  const dataWriter = new BufferDataWriter(Buffer.alloc(bufferSize), 4, bufferSizeParams)
  dataWriter.writeUInt16(params.key)
  dataWriter.writeUInt16(1)
  dataWriter.writeUInt32(params.correlationId || 101)
  dataWriter.writeUInt16(params.responseCode || 1)

  switch (params.key) {
    case PeerPropertiesResponse.key:
      dataWriter.writeInt32(0)
      break

    default:
      break
  }

  dataWriter.writePrefixSize()
  return dataWriter.toBuffer()
}
