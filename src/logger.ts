export interface Logger {
  debug(message: string): void
  info(message: string): void
  error(message: string): void
  warn(message: string): void
}

export class NullLogger implements Logger {
  debug(_message: string): void {
    // do nothing
  }
  info(_message: string): void {
    // do nothing
  }
  error(_message: string): void {
    // do nothing
  }
  warn(_message: string): void {
    // do nothing
  }
}
