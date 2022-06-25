import { inspect } from "node:util"
import { createLogger, format, transports } from "winston"

export function wait(timeout: number) {
  return new Promise((res) => setTimeout(res, timeout))
}

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
