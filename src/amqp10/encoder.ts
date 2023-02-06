import { inspect } from "node:util"
import { isDate } from "node:util/types"
import { Message, MessageApplicationProperties, MessageProperties } from "../producer"
import { DataWriter } from "../requests/data_writer"

const FormatCodeType = {
  MessageProperties: 0x73,
  ApplicationProperties: 0x74,
  ApplicationData: 0x75,
} as const

const FormatCode = {
  Described: 0x00,
  Vbin8: 0xa0,
  Str8: 0xa1,
  Sym8: 0xa3,
  Vbin32: 0xb0,
  Str32: 0xb1,
  Sym32: 0xb3,
  List32: 0xd0,
  Map32: 0xd1,
  Null: 0x40,
  SmallUlong: 0x53,
  Uint: 0x70,
  Int: 0x71,
  Timestamp: 0x83,
} as const

type MessageApplicationPropertiesList = [string, string | number][]

export function amqpEncode(writer: DataWriter, { content, properties, applicationProperties }: Message): void {
  const applicationPropertiesList = toList(applicationProperties)
  writer.writeUInt32(
    lengthOfContent(content) + lengthOfProperties(properties) + lengthOfApplicationProperties(applicationPropertiesList)
  )

  writeProperties(writer, properties)
  writeApplicationProperties(writer, applicationPropertiesList)
  writeContent(writer, content)
}

function lengthOfContent(content: Buffer) {
  return content.length + 3 + (content.length <= 255 ? 2 : 5)
}

function lengthOfProperties(properties?: MessageProperties) {
  if (!properties) return 0

  // header + FormatCode.List32 + value of getPropertySize() + count + size of all properties
  return 3 + 1 + 4 + 4 + getPropertySize(properties)
}

function lengthOfApplicationProperties(applicationProperties: MessageApplicationPropertiesList) {
  if (!applicationProperties.length) {
    return 0
  }

  // var size = DescribedFormatCode.Size
  // size += sizeof(byte) //FormatCode.List32
  // size += sizeof(uint) // field numbers
  // size += sizeof(uint) // PropertySize
  // size += MapSize()
  // return size
  return 3 + 1 + 4 + 4 + getApplicationPropertySize(applicationProperties)
}

function writeApplicationProperties(writer: DataWriter, applicationPropertiesList: MessageApplicationPropertiesList) {
  if (!applicationPropertiesList.length) {
    return
  }

  // write applicationData header
  writer.writeByte(FormatCode.Described)
  writer.writeByte(FormatCode.SmallUlong)
  writer.writeByte(FormatCodeType.ApplicationProperties)

  writer.writeByte(FormatCode.Map32)
  writer.writeUInt32(getApplicationPropertySize(applicationPropertiesList) + 3 + 1) // MapSize  + DescribedFormatCode + FormatCode
  writer.writeUInt32(applicationPropertiesList.length * 2)
  applicationPropertiesList
    .filter(([key]) => key)
    .forEach(([k, v]) => {
      amqpWriteString(writer, k)
      typeof v === "string" ? amqpWriteString(writer, v) : amqpWriteIntNumber(writer, v)
    })
}

function writeProperties(writer: DataWriter, properties?: MessageProperties) {
  if (!properties) {
    return
  }

  // write applicationData header
  writer.writeByte(FormatCode.Described)
  writer.writeByte(FormatCode.SmallUlong)
  writer.writeByte(FormatCodeType.MessageProperties)

  writer.writeByte(FormatCode.List32)
  writer.writeUInt32(getPropertySize(properties) + 3 + 1) // PropertySize
  writer.writeUInt32(13) // Always send everything
  amqpWriteString(writer, properties.messageId)
  amqpWriteBuffer(writer, properties.userId)
  amqpWriteString(writer, properties.to)
  amqpWriteString(writer, properties.subject)
  amqpWriteString(writer, properties.replyTo)
  amqpWriteString(writer, properties.correlationId)
  amqpWriteSymbol(writer, properties.contentType)
  amqpWriteSymbol(writer, properties.contentEncoding)
  amqpWriteDate(writer, properties.absoluteExpiryTime)
  amqpWriteDate(writer, properties.creationTime)
  amqpWriteString(writer, properties.groupId)
  amqpWriteNumber(writer, properties.groupSequence)
  amqpWriteString(writer, properties.replyToGroupId)
}

function writeContent(writer: DataWriter, content: Buffer) {
  // write applicationData header
  writer.writeByte(FormatCode.Described)
  writer.writeByte(FormatCode.SmallUlong)
  writer.writeByte(FormatCodeType.ApplicationData)

  // write data
  if (content.length <= 255) {
    writer.writeByte(FormatCode.Vbin8)
    writer.writeByte(content.length)
  } else {
    writer.writeByte(FormatCode.Vbin32)
    writer.writeUInt32(content.length)
  }
  writer.writeData(content)
}

function getPropertySize(properties: MessageProperties): number {
  return (
    getSizeOf(properties.messageId) +
    getSizeOf(properties.userId) +
    getSizeOf(properties.to) +
    getSizeOf(properties.subject) +
    getSizeOf(properties.replyTo) +
    getSizeOf(properties.correlationId) +
    getSizeOf(properties.contentType) +
    getSizeOf(properties.contentEncoding) +
    getSizeOf(properties.absoluteExpiryTime) +
    getSizeOf(properties.creationTime) +
    getSizeOf(properties.groupId) +
    getSizeOf(properties.groupSequence) +
    getSizeOf(properties.replyToGroupId)
  )
}

function getApplicationPropertySize(applicationProperties: MessageApplicationPropertiesList): number {
  return applicationProperties.reduce((sum, [k, v]) => sum + getSizeOf(k) + getSizeOf(v), 0)
}

function amqpWriteString(writer: DataWriter, data?: string): void {
  if (!data) {
    return amqpWriteNull(writer)
  }

  const content = Buffer.from(data)
  if (content.length <= 255) {
    writer.writeByte(FormatCode.Str8)
    writer.writeByte(content.length)
  } else {
    writer.writeByte(FormatCode.Str32)
    writer.writeInt32(content.length)
  }
  writer.writeData(data)
}

function amqpWriteSymbol(writer: DataWriter, data?: string): void {
  if (!data) {
    return amqpWriteNull(writer)
  }

  const content = Buffer.from(data)
  if (content.length <= 255) {
    writer.writeByte(FormatCode.Sym8)
    writer.writeByte(content.length)
  } else {
    writer.writeByte(FormatCode.Sym32)
    writer.writeInt32(content.length)
  }
  writer.writeData(data)
}

function amqpWriteNumber(writer: DataWriter, data?: number): void {
  if (!data) {
    return amqpWriteNull(writer)
  }

  writer.writeByte(FormatCode.Uint)
  writer.writeUInt32(data)
}

function amqpWriteIntNumber(writer: DataWriter, data?: number): void {
  if (!data) {
    return amqpWriteNull(writer)
  }

  writer.writeByte(FormatCode.Int)
  writer.writeInt32(data)
}

function amqpWriteBuffer(writer: DataWriter, data?: Buffer): void {
  if (!data || !data.length) {
    return amqpWriteNull(writer)
  }

  if (data.length < 256) {
    writer.writeByte(FormatCode.Vbin8)
    writer.writeByte(data.length)
  } else {
    writer.writeByte(FormatCode.Vbin32)
    writer.writeUInt32(data.length)
  }
  writer.writeData(data)
}

function amqpWriteNull(writer: DataWriter) {
  writer.writeByte(FormatCode.Null)
}
function amqpWriteDate(writer: DataWriter, date?: Date): void {
  if (!date) {
    return amqpWriteNull(writer)
  }

  writer.writeByte(FormatCode.Timestamp)
  writer.writeUInt64(BigInt(date.getTime()))
}

function getSizeOf(value?: string | Date | number | Buffer): number {
  if (!value) {
    return 1
  }

  if (typeof value === "string") {
    const count = Buffer.from(value).length
    return count <= 255 ? 1 + 1 + count : 1 + 4 + count
  }

  if (isDate(value)) {
    return 1 + 8
  }

  if (typeof value === "number") {
    return 1 + 4
  }

  if (Buffer.isBuffer(value)) {
    return value.length <= 255 ? 1 + 1 + value.length : 1 + 4 + value.length
  }

  throw new Error(`Unsupported type: ${inspect(value)}`)
}

function toList(applicationProperties?: MessageApplicationProperties): MessageApplicationPropertiesList {
  if (!applicationProperties) return []
  return Object.entries(applicationProperties)
}
