import { gunzipSync, gzipSync } from "node:zlib"

export enum CompressionType {
  None = 0,
  Gzip = 1,
  // Not implemented by default.
  // It is possible to add custom codec with StreamCompressionCodecs
  Snappy = 2,
  Lz4 = 3,
  Zstd = 4,
}

export interface Compression {
  getType(): CompressionType
  compress(data: Buffer): Buffer
  decompress(data: Buffer): Buffer
}

export class NoneCompression implements Compression {
  static create(): NoneCompression {
    return new NoneCompression()
  }

  getType(): CompressionType {
    return CompressionType.None
  }

  compress(data: Buffer): Buffer {
    return data
  }

  decompress(data: Buffer): Buffer {
    return data
  }
}

export class GzipCompression implements Compression {
  static create(): GzipCompression {
    return new GzipCompression()
  }

  getType(): CompressionType {
    return CompressionType.Gzip
  }

  compress(data: Buffer): Buffer {
    return gzipSync(data)
  }

  decompress(data: Buffer): Buffer {
    return gunzipSync(data)
  }
}
