import { inspect } from "node:util"
import { isDate } from "node:util/types"
import { Message, MessageProperties } from "../producer"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

interface PublishRequestParams {
  publisherId: number
  messages: Array<{ publishingId: bigint; message: Message }>
}

export class PublishRequest extends AbstractRequest {
  readonly key = 0x02
  readonly responseKey = -1

  constructor(private params: PublishRequestParams) {
    super()
  }

  protected writeContent(writer: DataWriter): void {
    writer.writeUInt8(this.params.publisherId)
    writer.writeUInt32(this.params.messages.length)
    this.params.messages.forEach(({ publishingId, message }) => {
      writer.writeUInt64(publishingId)
      writer.writeUInt32(
        message.content.length + 3 + (message.content.length <= 255 ? 2 : 5) + lengthOfProperties(message.properties)
      )
      amqpEncode(writer, message)
    })
  }
}

const FormatCodeType = {
  MessageProperties: 0x73,
  ApplicationData: 0x75,
}

const FormatCode = {
  Described: 0x00,
  Vbin8: 0xa0,
  Str8: 0xa1,
  Sym8: 0xa3,
  Vbin32: 0xb0,
  Str32: 0xb1,
  Sym32: 0xb3,
  List32: 0xd0,
  Null: 0x40,
  SmallUlong: 0x53,
  Uint: 0x70,
  Timestamp: 0x83,
}

function amqpEncode(writer: DataWriter, { content, properties }: Message): void {
  writeProperties(writer, properties)
  writeContent(writer, content)
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

function lengthOfProperties(properties?: MessageProperties) {
  if (!properties) return 0

  // header + FormatCode.List32 + value of getPropertySize() + count + size of all properties
  return 3 + 1 + 4 + 4 + getPropertySize(properties)
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
