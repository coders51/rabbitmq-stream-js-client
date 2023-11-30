import { amqpEncode } from "./amqp10/encoder"
import { Message } from "./producer"
import { DataWriter } from "./requests/data_writer"

export interface Compression {
  type: number
  messages: Message[]

  compressedSize(): number
  unCompressedSize(): number
  messageCount(): number
  writeContent(writer: DataWriter): void
  compress(messages: Message[]): void
}

export class NoneCompression implements Compression {
  type = 0
  messages: Message[] = []

  static create(): NoneCompression {
    return new NoneCompression()
  }

  compress(messages: Message[]): void {
    this.messages = messages
  }

  compressedSize(): number {
    return this.messages.reduce((sum, message) => sum + 4 + message.content.length, 0)
  }

  unCompressedSize(): number {
    return this.messages.reduce((sum, message) => sum + 4 + message.content.length, 0)
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
