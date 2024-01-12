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
          const formatCode = dataResponse.readUInt8()
          dataResponse.rewind(1)
          const decodedBoolean = decodeFormatCode(dataResponse, formatCode)
          if (!decodedBoolean) throw new Error(`invalid formatCode %#02x: ${formatCode}`)
          acc.durable = decodedBoolean as boolean
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
          acc.firstAcquirer = decodeBooleanType(dataResponse, true)
          break
        case 4:
          acc.deliveryCount = dataResponse.readUInt32()
          break
        default:
          throw new Error(`PropertiesError`)
      }
      return acc
    }, {})
  }
}
