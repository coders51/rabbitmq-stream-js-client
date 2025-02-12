import { MessageProperties } from "../publisher"
import { decodeFormatCode } from "../response_decoder"
import { DataReader } from "../responses/raw_response"
import { range } from "../util"
import { FormatCode } from "./decoder"

export class Properties {
  public static parse(dataResponse: DataReader, fields: number): MessageProperties {
    return range(fields).reduce((acc: MessageProperties, index) => {
      if (dataResponse.isAtEnd()) return acc
      const formatCode = dataResponse.readUInt8()
      if (formatCode === FormatCode.Null) {
        return acc
      }
      switch (index) {
        case 0:
          acc.messageId = decodeFormatCode(dataResponse, formatCode) as string
          break
        case 1:
          // Reading of binary type
          const userIdLength = dataResponse.readUInt8()
          acc.userId = dataResponse.readBufferOf(userIdLength)
          break
        case 2:
          acc.to = decodeFormatCode(dataResponse, formatCode) as string
          break
        case 3:
          acc.subject = decodeFormatCode(dataResponse, formatCode) as string
          break
        case 4:
          acc.replyTo = decodeFormatCode(dataResponse, formatCode) as string
          break
        case 5:
          acc.correlationId = decodeFormatCode(dataResponse, formatCode) as string
          break
        case 6:
          acc.contentType = decodeFormatCode(dataResponse, formatCode) as string
          break
        case 7:
          acc.contentEncoding = decodeFormatCode(dataResponse, formatCode) as string
          break
        case 8:
          acc.absoluteExpiryTime = new Date(Number(dataResponse.readInt64()))
          break
        case 9:
          acc.creationTime = new Date(Number(dataResponse.readInt64()))
          break
        case 10:
          acc.groupId = decodeFormatCode(dataResponse, formatCode) as string
          break
        case 11:
          acc.groupSequence = dataResponse.readUInt32()
          break
        case 12:
          acc.replyToGroupId = decodeFormatCode(dataResponse, formatCode) as string
          break
        default:
          throw new Error(`PropertiesError`)
      }
      return acc
    }, {})
  }
}
