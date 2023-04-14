import { inspect } from "node:util"
import { createLogger, format, transports } from "winston"
import * as ampq from "amqplib"

export function removeFrom<T>(l: T[], predicate: (x: T) => boolean): T | undefined {
  const i = l.findIndex(predicate)
  if (i === -1) return
  const [e] = l.splice(i, 1)
  return e
}

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
