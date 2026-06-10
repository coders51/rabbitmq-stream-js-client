import { gunzipSync, gzipSync } from "node:zlib"

/**
 * Compression algorithms supported when publishing sub-batch entries
 *
 * Only `None` and `Gzip` are implemented by default. The remaining values
 * are defined by the stream protocol but require a custom codec to be used.
 */
export enum CompressionType {
  None = 0,
  Gzip = 1,
  // Not implemented by default.
  // It is possible to add custom codec with StreamCompressionCodecs
  Snappy = 2,
  Lz4 = 3,
  Zstd = 4,
}

/**
 * A compression codec used to compress and decompress sub-batch entries
 */
export interface Compression {
  /** The compression type this codec implements */
  getType(): CompressionType
  /** Compress the given buffer */
  compress(data: Buffer): Buffer
  /** Decompress the given buffer */
  decompress(data: Buffer): Buffer
}

/**
 * A no-op compression codec that leaves the data unchanged
 */
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

/**
 * A compression codec backed by Node's built-in gzip implementation
 */
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
