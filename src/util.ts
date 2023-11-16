import { inspect } from "node:util"
import { createLogger, format, transports } from "winston"
import { LoggerParams } from "."

export function removeFrom<T>(l: T[], predicate: (x: T) => boolean): T | undefined {
  const i = l.findIndex(predicate)
  if (i === -1) return
  const [e] = l.splice(i, 1)
  return e
}

export function createConsoleLog(loggerParams?: LoggerParams) {
  const { silent, level } = loggerParams
    ? { silent: loggerParams === "none", level: loggerParams }
    : { silent: false, level: "info" }

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

export function range(count: number): number[] {
  const ret = Array(count)
  for (let index = 0; index < count; index++) {
    ret[index] = index
  }
  return ret
}
