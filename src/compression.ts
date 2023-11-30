import { amqpEncode, messageSize } from "./amqp10/encoder"
import { Message } from "./producer"
import { DataWriter } from "./requests/data_writer"

export enum CompressionType {
  None = 0,
  GZip = 1,
  // Not implemented by default.
  // It is possible to add custom codec with StreamCompressionCodecs
  Snappy = 2,
  Lz4 = 3,
  Zstd = 4,
}

export interface Compression {
  type: CompressionType
  messages: Message[]

  compressedSize(): number
  unCompressedSize(): number
  messageCount(): number
  writeContent(writer: DataWriter): void
  compress(messages: Message[]): void
}

export class NoneCompression implements Compression {
  type = CompressionType.None
  messages: Message[] = []

  static create(): NoneCompression {
    return new NoneCompression()
  }

  compress(messages: Message[]): void {
    this.messages = messages
  }

  compressedSize(): number {
    return this.messages.reduce((sum, message) => sum + 4 + messageSize(message), 0)
  }

  unCompressedSize(): number {
    return this.messages.reduce((sum, message) => sum + 4 + messageSize(message), 0)
  }

  messageCount(): number {
    return this.messages.length
  }

  writeContent(writer: DataWriter): void {
    this.messages.forEach((message) => {
      amqpEncode(writer, message)
    })
  }
}

// export class GZipCompression implements Compression {
//   type: CompressionType = CompressionType.GZip
//   messages: Message[] = []

//   compress(messages: Message[]): void {
//     throw new Error("Method not implemented.")
//   }

//   CompressedSize(): number {
//     throw new Error("Method not implemented.")
//   }

//   UnCompressedSize(): number {
//     throw new Error("Method not implemented.")
//   }

//   writeContent(writer: DataWriter): void {
//     throw new Error("Method not implemented.")
//   }

//   messageCount(): number {
//     // return this.messages.length
//     throw new Error("Method not implemented.")
//   }
// }
