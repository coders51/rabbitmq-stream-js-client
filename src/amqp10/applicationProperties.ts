import { MessageApplicationProperties } from "../producer"
import { DataReader } from "../responses/raw_response"
import { FormatCode } from "./decoder"
import { range } from "../util"
import { readUTF8String } from "../response_decoder"

export class ApplicationProperties {
  public static parse(dataReader: DataReader, elementsLength: number): MessageApplicationProperties {
    const numEntries = elementsLength / 2

    return range(numEntries).reduce((acc: MessageApplicationProperties, _) => {
      const propertyKey = readUTF8String(dataReader)
      const nextByteType = dataReader.readUInt8()
      dataReader.rewind(1)
      acc[propertyKey] = ApplicationProperties.decodeNextByteType(dataReader, nextByteType)
      return acc
    }, {})
  }

  private static decodeNextByteType(dataReader: DataReader, nextByteType: number) {
    switch (nextByteType) {
      case FormatCode.Sym32:
      case FormatCode.Sym8:
      case FormatCode.Str8:
      case FormatCode.Str32:
        return readUTF8String(dataReader)
      case FormatCode.Uint0:
        return 0
      case FormatCode.SmallUint:
        // Skipping formatCode
        dataReader.forward(1)
        return dataReader.readUInt8()
      case FormatCode.Uint:
        // Skipping formatCode
        dataReader.forward(1)
        return dataReader.readUInt32()
      case FormatCode.SmallInt:
        // Skipping formatCode
        dataReader.forward(1)
        return dataReader.readInt8()
      case FormatCode.Int:
        // Skipping formatCode
        dataReader.forward(1)
        return dataReader.readInt32()
      default:
        return ""
    }
  }
}
