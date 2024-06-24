import * as amqp from "amqplib"
import { AssertionError, expect } from "chai"
import { inspect } from "node:util"
import { createLogger, format, transports } from "winston"
import { ApplicationProperties } from "../../src/amqp10/applicationProperties"
import { FormatCodeType } from "../../src/amqp10/decoder"
import { Header } from "../../src/amqp10/messageHeader"
import { Properties } from "../../src/amqp10/properties"
import { Message, MessageApplicationProperties, MessageHeader, MessageProperties, Publisher } from "../../src/publisher"
import { decodeFormatCode } from "../../src/response_decoder"
import { DataReader } from "../../src/responses/raw_response"

export function createConsoleLog({ silent, level } = { silent: false, level: "debug" }) {
  return createLogger({
    silent,
    level,
    format: format.combine(
      format.colorize(),
      format.timestamp(),
      format.align(),
      format.splat(),
      format.label(),
      format.printf((info) => `${info.timestamp} ${info.level}: ${info.message} ${info.meta ? inspect(info.meta) : ""}`)
    ),
    transports: new transports.Console(),
  })
}

const getAmqpConnectionString = (user: string, pwd: string): string => {
  const [firstNode] = getTestNodesFromEnv()
  const port = process.env.RABBIT_MQ_AMQP_PORT ?? 5672
  return `amqp://${user}:${pwd}@${firstNode.host}:${port}/%2F`
}

export function elapsedFrom(from: number): number {
  return Date.now() - from
}

export async function always(fn: Function, timeout = 1500) {
  const start = Date.now()
  while (true) {
    try {
      await fn()
      await wait(5)
      if (elapsedFrom(start) > timeout) {
        return
      }
    } catch (error) {
      if (error instanceof AssertionError) throw error
      expect.fail(error as string)
    }
  }
}

export async function eventually(fn: Function, timeout = 1500) {
  const start = Date.now()
  while (true) {
    try {
      await fn()
      return
    } catch (error) {
      if (elapsedFrom(start) > timeout) {
        if (error instanceof AssertionError) throw error
        expect.fail(error as string)
      }
      await wait(5)
    }
  }
}

export async function expectToThrowAsync(
  method: () => Promise<unknown>,
  expectedError: Function | Error,
  errorMessage: string | RegExp | null = null
): Promise<void> {
  let error = null
  try {
    await method()
  } catch (err) {
    error = err
  }
  expect(error).instanceOf(expectedError)
  if (errorMessage instanceof RegExp) {
    expect((error as { message: string }).message).match(errorMessage)
  }
  if (typeof errorMessage === "string") {
    expect((error as { message: string }).message).eql(errorMessage)
  }
}

export function wait(timeout: number) {
  return new Promise((res) => setTimeout(res, timeout))
}

export async function getMessageFrom(
  stream: string,
  user: string,
  pwd: string
): Promise<{ content: string; properties: amqp.MessageProperties }> {
  return new Promise(async (res, rej) => {
    const con = await amqp.connect(getAmqpConnectionString(user, pwd)).catch((e) => {
      console.error("Could not connect to Rabbit due to:", e)
      rej(e)
    })
    if (!con) {
      throw new Error(`Connection with AMPQ could not be established`)
    }
    con.on("error", async (err) => rej(err))
    const ch = await con.createChannel()
    await ch.prefetch(1)
    await ch.consume(
      stream,
      async (msg) => {
        if (!msg) return
        msg.properties.userId
        ch.ack(msg)
        await ch.close()
        await con.close()
        res({ content: msg.content.toString(), properties: msg.properties })
      },
      { arguments: { "x-stream-offset": "first" } }
    )
  })
}

export async function createClassicConsumer(
  stream: string,
  cb: (msg: amqp.Message) => void
): Promise<{ conn: amqp.Connection; ch: amqp.Channel }> {
  const conn = await amqp.connect(getAmqpConnectionString(username, password))
  const ch = await conn.createChannel()
  await ch.prefetch(1)
  await ch.consume(
    stream,
    async (msg) => {
      if (!msg) return
      cb(msg)
      ch.ack(msg)
    },
    { arguments: { "x-stream-offset": "first" } }
  )

  return { conn, ch }
}

export async function createClassicPublisher(): Promise<{ conn: amqp.Connection; ch: amqp.Channel }> {
  const conn = await amqp.connect(getAmqpConnectionString(username, password))
  const ch = await conn.createChannel()
  return { conn, ch }
}

export function decodeMessageTesting(dataResponse: DataReader, length: number): Message {
  let content = Buffer.from("")
  let messageProperties: MessageProperties = {}
  let messageHeader: MessageHeader = {}
  let amqpValue: string = ""
  let applicationProperties: MessageApplicationProperties = {}
  while (dataResponse.position() < length) {
    dataResponse.readUInt8()
    dataResponse.readUInt8()
    const formatCode = dataResponse.readUInt8()
    switch (formatCode) {
      case FormatCodeType.ApplicationData:
        const formatCodeApplicationData = dataResponse.readUInt8()
        const lenApplicationData = decodeFormatCode(dataResponse, formatCodeApplicationData)
        if (!length) throw new Error(`invalid formatCode %#02x: ${formatCodeApplicationData}`)
        content = dataResponse.readBufferOf(lenApplicationData as number)
        break
      case FormatCodeType.MessageProperties:
        dataResponse.rewind(3)
        const typeMessageProperties = dataResponse.readInt8()
        if (typeMessageProperties !== 0) {
          throw new Error(`invalid composite header %#02x: ${typeMessageProperties}`)
        }
        const nextTypeMessageProperties = dataResponse.readInt8()
        decodeFormatCode(dataResponse, nextTypeMessageProperties)
        const formatCodeMessageProperties = dataResponse.readUInt8()
        const propertiesLength = decodeFormatCode(dataResponse, formatCodeMessageProperties)
        if (!propertiesLength) throw new Error(`invalid formatCode %#02x: ${formatCodeMessageProperties}`)
        messageProperties = Properties.parse(dataResponse, propertiesLength as number)
        break
      case FormatCodeType.ApplicationProperties:
        const formatCodeApplicationProperties = dataResponse.readUInt8()
        const applicationPropertiesLength = decodeFormatCode(dataResponse, formatCodeApplicationProperties)
        if (!applicationPropertiesLength)
          throw new Error(`invalid formatCode %#02x: ${formatCodeApplicationProperties}`)
        applicationProperties = ApplicationProperties.parse(dataResponse, applicationPropertiesLength as number)
        break
      case FormatCodeType.MessageHeader:
        dataResponse.rewind(3)
        const typeMessageHeader = dataResponse.readInt8()
        if (typeMessageHeader !== 0) {
          throw new Error(`invalid composite header %#02x: ${typeMessageHeader}`)
        }
        const nextMessageHeaderType = dataResponse.readInt8()
        decodeFormatCode(dataResponse, nextMessageHeaderType)
        const formatCodeHeader = dataResponse.readUInt8()
        const headerLength = decodeFormatCode(dataResponse, formatCodeHeader)
        if (!headerLength) throw new Error(`invalid formatCode %#02x: ${formatCodeHeader}`)
        messageHeader = Header.parse(dataResponse, headerLength as number)
        break
      case FormatCodeType.AmqpValue:
        const amqpFormatCode = dataResponse.readUInt8()
        dataResponse.rewind(1)
        amqpValue = decodeFormatCode(dataResponse, amqpFormatCode, true) as string
        break
      default:
        throw new Error(`Not supported format code ${formatCode}`)
    }
  }

  return { content, messageProperties, messageHeader, applicationProperties, amqpValue, offset: BigInt(length) }
}

export const sendANumberOfRandomMessages = async (publisher: Publisher, offset = 0): Promise<string[]> => {
  const noOfMessages = Math.floor(Math.random() * 10) + 1
  const messages = Array.from(Array(noOfMessages).keys()).map((_, i) => `Message number ${i + offset + 1}`)
  await Promise.all(messages.map((m) => publisher.send(Buffer.from(m))))
  return messages
}

export const getTestNodesFromEnv = (): { host: string; port: number }[] => {
  const envValue = process.env.RABBIT_MQ_TEST_NODES ?? "localhost:5552"
  const nodes = envValue.split(";")
  return nodes.map((n) => {
    const [host, port] = n.split(":")
    return { host: host ?? "localhost", port: parseInt(port) ?? 5552 }
  })
}

export const waitSleeping = async (ms: number) => {
  return new Promise((res) => {
    setTimeout(() => res(true), ms)
  })
}

export async function mapSync<T, U>(l: T[], fn: (value: T) => Promise<U>): Promise<U[]> {
  const res: U[] = []
  for (const e of l) {
    res.push(await fn(e))
  }
  return res
}

export const username = process.env.RABBITMQ_USER || "rabbit"
export const password = process.env.RABBITMQ_PASSWORD || "rabbit"
