import { MetadataInfo } from "./responses/raw_response"
import { Logger } from "winston"

export class Metadata {
  private metadataCode = -1
  private metadataStrean = ""

  constructor(private readonly logger: Logger) {}

  setTargetMetadata(metadataInfo: MetadataInfo) {
    this.metadataCode = metadataInfo.code
    this.metadataStrean = metadataInfo.stream
    this.logger.debug(`Target metadata to update -- Stream: ${this.metadataStrean}; Code: ${this.metadataCode}`)
  }
}
