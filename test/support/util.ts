import { AssertionError, expect } from "chai"
import * as ampq from "amqplib"

export function elapsedFrom(from: number): number {
  return Date.now() - from
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

export async function getMessageFrom(stream: string): Promise<{ content: string; properties: ampq.MessageProperties }> {
  return new Promise(async (res, rej) => {
    const con = await ampq.connect("amqp://rabbit:rabbit@localhost")
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
  cb: (msg: ampq.Message) => void
): Promise<{ conn: ampq.Connection; ch: ampq.Channel }> {
  const conn = await ampq.connect("amqp://rabbit:rabbit@localhost")
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
export const username = process.env.RABBIT_USER || "rabbit"
export const password = process.env.RABBIT_PASSWORD || "rabbit"
