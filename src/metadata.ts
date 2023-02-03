import { MetadataInfo } from "./responses/raw_response"
import { Logger } from "winston"

export class Metadata {
  private metadataCode = -1
  private metadataStream = ""

  constructor(private readonly logger: Logger) {}

  setTargetMetadata(metadataInfo: MetadataInfo) {
    this.metadataCode = metadataInfo.code
    this.metadataStream = metadataInfo.stream
    this.logger.debug(`Target metadata to update -- Stream: ${this.metadataStream}; Code: ${this.metadataCode}`)
  }
}
