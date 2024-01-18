import { MessageProperties } from "../publisher"
import { readUTF8String } from "../response_decoder"
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
      dataResponse.rewind(1)
      switch (index) {
        case 0:
          acc.messageId = readUTF8String(dataResponse)
          break
        case 1:
          // Reading of binary type
          dataResponse.readUInt8()
          const userIdLength = dataResponse.readUInt8()
          acc.userId = dataResponse.readBufferOf(userIdLength)
          break
        case 2:
          acc.to = readUTF8String(dataResponse)
          break
        case 3:
          acc.subject = readUTF8String(dataResponse)
          break
        case 4:
          acc.replyTo = readUTF8String(dataResponse)
          break
        case 5:
          acc.correlationId = readUTF8String(dataResponse)
          break
        case 6:
          acc.contentType = readUTF8String(dataResponse)
          break
        case 7:
          acc.contentEncoding = readUTF8String(dataResponse)
          break
        case 8:
          dataResponse.readUInt8()
          acc.absoluteExpiryTime = new Date(Number(dataResponse.readInt64()))
          break
        case 9:
          dataResponse.readUInt8()
          acc.creationTime = new Date(Number(dataResponse.readInt64()))
          break
        case 10:
          acc.groupId = readUTF8String(dataResponse)
          break
        case 11:
          dataResponse.readUInt8()
          acc.groupSequence = dataResponse.readUInt32()
          break
        case 12:
          acc.replyToGroupId = readUTF8String(dataResponse)
          break
        default:
          throw new Error(`PropertiesError`)
      }
      return acc
    }, {})
  }
}
