import { MessageProperties } from "../producer"
import { DataReader } from "../responses/raw_response"
import { range } from "../util"

export class Properties {
  public static parse(dataResponse: DataReader, fields: number): MessageProperties {
    return range(fields).reduce((acc: MessageProperties, index) => {
      if (!dataResponse.isAtEnd()) {
        switch (index) {
          case 0:
            acc.messageId = dataResponse.readUTF8String()
            break
          case 1:
            // Reading of binary type
            dataResponse.readUInt8()
            const userIdLength = dataResponse.readUInt8()
            acc.userId = dataResponse.readBufferOf(userIdLength)
            break
          case 2:
            acc.to = dataResponse.readUTF8String()
            break
          case 3:
            acc.subject = dataResponse.readUTF8String()
            break
          case 4:
            acc.replyTo = dataResponse.readUTF8String()
            break
          case 5:
            acc.correlationId = dataResponse.readUTF8String()
            break
          case 6:
            acc.contentType = dataResponse.readUTF8String()
            break
          case 7:
            acc.contentEncoding = dataResponse.readUTF8String()
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
            acc.groupId = dataResponse.readUTF8String()
            break
          case 11:
            dataResponse.readUInt8()
            acc.groupSequence = dataResponse.readUInt32()
            break
          case 12:
            acc.replyToGroupId = dataResponse.readUTF8String()
            break
          default:
            throw new Error(`PropertiesError`)
        }
      }
      return acc
    }, {})
  }
}
