import { DataReader } from "../responses/raw_response"
import { MessageHeader } from "../publisher"
import { range } from "../util"
import { decodeFormatCode, decodeBooleanType } from "../response_decoder"

export class Header {
  public static parse(dataResponse: DataReader, fields: number): MessageHeader {
    return range(fields).reduce((acc: MessageHeader, index) => {
      if (dataResponse.isAtEnd()) return acc
      switch (index) {
        case 0:
          acc.durable = decodeBooleanType(dataResponse, dataResponse.readUInt8())
          break
        case 1:
          dataResponse.readUInt8() // Read type
          acc.priority = dataResponse.readUInt8()
          break
        case 2:
          const type = dataResponse.readUInt8()
          acc.ttl = decodeFormatCode(dataResponse, type) as number
          break
        case 3:
          acc.firstAcquirer = decodeBooleanType(dataResponse, dataResponse.readUInt8())
          break
        case 4:
          acc.deliveryCount = decodeFormatCode(dataResponse, dataResponse.readUInt8()) as number
          break
        default:
          throw new Error(`HeaderError`)
      }
      return acc
    }, {})
  }
}
