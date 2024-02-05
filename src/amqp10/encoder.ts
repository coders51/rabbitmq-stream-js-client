import { inspect } from "node:util"
import { isDate } from "node:util/types"
import {
  AmqpByte,
  Message,
  MessageAnnotations,
  MessageAnnotationsValue,
  MessageApplicationProperties,
  MessageProperties,
} from "../publisher"
import { DataWriter } from "../requests/data_writer"

const FormatCodeType = {
  Annotations: 0x72,
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
  Ubyte: 0x50,
  Int: 0x71,
  Timestamp: 0x83,
} as const

const PropertySizeDescription =
  3 + // sizeOf DescribedFormatCode.Size (3 byte)
  1 + // sizeOf FormatCode.List32 (byte)
  4 + // sizeOf field numbers (uint)
  4 // sizeof propertySize (uint)

type MessageApplicationPropertiesList = { key: string; value: string | number }[]

type MessageAnnotationsList = { key: string; value: MessageAnnotationsValue }[]

export function amqpEncode(
  writer: DataWriter,
  { content, messageProperties, applicationProperties, messageAnnotations }: Message
): void {
  writer.writeUInt32(messageSize({ content, messageProperties, applicationProperties, messageAnnotations }))
  writeMessageAnnotations(writer, toAnnotationsList(messageAnnotations))
  writeProperties(writer, messageProperties)
  writeApplicationProperties(writer, toList(applicationProperties))
  writeContent(writer, content)
}

export function messageSize({ content, messageProperties, applicationProperties, messageAnnotations }: Message) {
  return (
    lengthOfContent(content) +
    lengthOfProperties(messageProperties) +
    lengthOfApplicationProperties(toList(applicationProperties)) +
    lengthOfMessageAnnotations(toAnnotationsList(messageAnnotations))
  )
}

function lengthOfContent(content: Buffer) {
  return content.length + 3 + (content.length <= 255 ? 2 : 5)
}

function lengthOfProperties(properties?: MessageProperties) {
  if (!properties) return 0

  return PropertySizeDescription + getPropertySize(properties)
}

function lengthOfApplicationProperties(applicationProperties: MessageApplicationPropertiesList) {
  if (!applicationProperties.length) {
    return 0
  }

  return PropertySizeDescription + getListSize(applicationProperties)
}

function lengthOfMessageAnnotations(messageAnnotations: MessageAnnotationsList) {
  if (!messageAnnotations.length) {
    return 0
  }

  return PropertySizeDescription + getListSize(messageAnnotations)
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
  writer.writeUInt32(getListSize(applicationPropertiesList) + 3 + 1) // MapSize  + DescribedFormatCode + FormatCode
  writer.writeUInt32(applicationPropertiesList.length * 2)
  applicationPropertiesList
    .filter((elem) => elem.key)
    .forEach((elem) => {
      amqpWriteString(writer, elem.key)
      typeof elem.value === "string" ? amqpWriteString(writer, elem.value) : amqpWriteIntNumber(writer, elem.value)
    })
}

function writeMessageAnnotations(writer: DataWriter, messageAnnotationsList: MessageAnnotationsList) {
  if (!messageAnnotationsList.length) {
    return
  }

  // write applicationData header
  writer.writeByte(FormatCode.Described)
  writer.writeByte(FormatCode.SmallUlong)
  writer.writeByte(FormatCodeType.Annotations)

  writer.writeByte(FormatCode.Map32)
  writer.writeUInt32(getListSize(messageAnnotationsList) + 3 + 1) // MapSize  + DescribedFormatCode + FormatCode
  writer.writeUInt32(messageAnnotationsList.length * 2)
  messageAnnotationsList
    .filter((elem) => elem.key)
    .forEach((elem) => {
      amqpWriteString(writer, elem.key)
      if (elem.value instanceof AmqpByte) {
        amqpWriteByte(writer, elem.value)
      } else {
        typeof elem.value === "string" ? amqpWriteString(writer, elem.value) : amqpWriteIntNumber(writer, elem.value)
      }
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

function getListSize(list: MessageAnnotationsList): number {
  return list.reduce(
    (sum: number, elem: { key: string; value: MessageAnnotationsValue }) =>
      sum + getSizeOf(elem.key) + getSizeOf(elem.value),
    0
  )
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

function amqpWriteByte(writer: DataWriter, data: AmqpByte): void {
  writer.writeByte(FormatCode.Ubyte)
  writer.writeByte(data.byteValue)
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

function getSizeOf(value?: string | Date | number | Buffer | AmqpByte): number {
  if (!value) {
    return 1
  }

  if (value instanceof AmqpByte) {
    return 1 + 1
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
  return Object.entries(applicationProperties).map((elem) => {
    return { key: elem[0], value: elem[1] }
  })
}

function toAnnotationsList(annotations?: MessageAnnotations): MessageAnnotationsList {
  if (!annotations) return []
  return Object.entries(annotations).map((elem) => {
    return { key: elem[0], value: elem[1] }
  })
}
