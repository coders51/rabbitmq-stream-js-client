import { MessageProperties } from "../producer"
import { DataReader } from "../responses/raw_response"

export class Properties {
  public static Parse(dataResponse: DataReader, fields: number): MessageProperties {
    let messageId = ""
    let to = ""
    let subject = ""
    let replyTo = ""
    let correlationId = ""
    let contentType = ""
    let contentEncoding = ""
    let userId = Buffer.from("")
    let absoluteExpiryTime = new Date()
    let creationTime = new Date()
    let groupId = ""
    let groupSequence = 0
    let replyToGroupId = ""

    for (let index = 0; index < fields; index++) {
      if (!dataResponse.atEnd()) {
        switch (index) {
          case 0:
            messageId = dataResponse.readUTF8String()
            break
          case 1:
            // Reading of binary type
            dataResponse.readUInt8()
            const userIdLength = dataResponse.readUInt8()
            userId = dataResponse.readBufferOf(userIdLength)
            break
          case 2:
            to = dataResponse.readUTF8String()
            break
          case 3:
            subject = dataResponse.readUTF8String()
            break
          case 4:
            replyTo = dataResponse.readUTF8String()
            break
          case 5:
            correlationId = dataResponse.readUTF8String()
            break
          case 6:
            contentType = dataResponse.readUTF8String()
            break
          case 7:
            contentEncoding = dataResponse.readUTF8String()
            break
          case 8:
            dataResponse.readUInt8()
            absoluteExpiryTime = new Date(Number(dataResponse.readInt64()))
            break
          case 9:
            dataResponse.readUInt8()
            creationTime = new Date(Number(dataResponse.readInt64()))
            break
          case 10:
            groupId = dataResponse.readUTF8String()
            break
          case 11:
            dataResponse.readUInt8()
            groupSequence = dataResponse.readUInt32()
            break
          case 12:
            replyToGroupId = dataResponse.readUTF8String()
            break
          default:
            throw new Error(`PropertiesError`)
        }
      }
    }

    const retProperties: MessageProperties = {
      contentType,
      contentEncoding,
      messageId,
      replyTo,
      to,
      subject,
      correlationId,
      userId,
      absoluteExpiryTime,
      creationTime,
      groupId,
      groupSequence,
      replyToGroupId,
    }

    return retProperties
  }
}
