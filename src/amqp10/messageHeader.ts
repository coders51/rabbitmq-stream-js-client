import { DataReader } from "../responses/raw_response"
import { MessageHeader } from "../publisher"
import { range } from "../util"
import { decodeFormatCode, decodeBooleanType } from "../response_decoder"
import { FormatCode } from "./decoder"

export class Header {
  public static parse(dataResponse: DataReader, fields: number): MessageHeader {
    return range(fields).reduce((acc: MessageHeader, index) => {
      if (dataResponse.isAtEnd()) return acc

      const type = dataResponse.readUInt8()
      if (type !== FormatCode.Null) {
        switch (index) {
          case 0:
            acc.durable = decodeBooleanType(dataResponse, type)
            break
          case 1:
            acc.priority = decodeFormatCode(dataResponse, type) as number
            break
          case 2:
            acc.ttl = decodeFormatCode(dataResponse, type) as number
            break
          case 3:
            acc.firstAcquirer = decodeBooleanType(dataResponse, type)
            break
          case 4:
            acc.deliveryCount = decodeFormatCode(dataResponse, type) as number
            break
          default:
            throw new Error(`HeaderError`)
        }
      }
      return acc
    }, {})
  }
}
