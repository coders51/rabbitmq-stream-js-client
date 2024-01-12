import { MessageApplicationProperties } from "../publisher"
import { DataReader } from "../responses/raw_response"
import { range } from "../util"
import { readUTF8String, decodeFormatCode } from "../response_decoder"

export class ApplicationProperties {
  public static parse(dataReader: DataReader, elementsLength: number): MessageApplicationProperties {
    const numEntries = elementsLength / 2

    return range(numEntries).reduce((acc: MessageApplicationProperties, _) => {
      const propertyKey = readUTF8String(dataReader)
      const nextByteType = dataReader.readUInt8()
      dataReader.rewind(1)
      const propertyValue = decodeFormatCode(dataReader, nextByteType, true)
      if (!propertyValue) throw new Error(`invalid nextByteType %#02x: ${nextByteType}`)
      acc[propertyKey] = propertyValue as string | number
      return acc
    }, {})
  }
}
