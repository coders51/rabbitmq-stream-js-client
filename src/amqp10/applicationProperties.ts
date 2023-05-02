import { MessageApplicationProperties } from "../producer"
import { DataReader } from "../responses/raw_response"
import { FormatCode } from "./decoder"

export class ApplicationProperties {
  public static Parse(dataReader: DataReader, elementsLength: number): MessageApplicationProperties {
    const numEntries = elementsLength / 2
    const messageApplicationProperties: MessageApplicationProperties = {}

    for (let index = 0; index < numEntries; index++) {
      const propertyKey = dataReader.readUTF8String()
      let propertyValue: string | number
      const nextByteType = dataReader.readUInt8()
      dataReader.rewind(1)

      switch (nextByteType) {
        case FormatCode.Sym32:
        case FormatCode.Sym8:
        case FormatCode.Str8:
        case FormatCode.Str32:
          propertyValue = dataReader.readUTF8String()
          break
        case FormatCode.Uint0:
          propertyValue = 0
          break
        case FormatCode.SmallUint:
          // Skipping formatCode
          dataReader.forward(1)
          propertyValue = dataReader.readUInt8()
          break
        case FormatCode.Uint:
          // Skipping formatCode
          dataReader.forward(1)
          propertyValue = dataReader.readUInt32()
          break
        case FormatCode.SmallInt:
          // Skipping formatCode
          dataReader.forward(1)
          propertyValue = dataReader.readInt8()
          break
        case FormatCode.Int:
          // Skipping formatCode
          dataReader.forward(1)
          propertyValue = dataReader.readInt32()
          break
        default:
          propertyValue = ""
          break
      }
      messageApplicationProperties[propertyKey] = propertyValue
    }

    return messageApplicationProperties
  }
}
