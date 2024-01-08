import { AbstractResponse } from "./abstract_response"
import { DataReader, RawResponse } from "./raw_response"

export interface Broker {
  reference: number
  host: string
  port: number
}

export interface StreamMetadata {
  streamName: string
  responseCode: number
  leader?: Broker
  replicas?: Broker[]
}

export class MetadataResponse extends AbstractResponse {
  static key = 0x800f as const
  static readonly Version = 1

  readonly streamInfos: StreamMetadata[] = []

  constructor(response: RawResponse) {
    super(response)
    this.verifyKey(MetadataResponse)

    const payload = response.payload

    const brokers: Broker[] = []

    const noOfBrokers = payload.readInt32()
    for (let i = 0; i < noOfBrokers; i++) {
      brokers.push({
        reference: payload.readUInt16(),
        host: payload.readString(),
        port: payload.readUInt32(),
      })
    }

    const noOfStreamInfos = payload.readInt32()
    for (let i = 0; i < noOfStreamInfos; i++) {
      const streamName = payload.readString()
      const streamInfo = {
        streamName,
        responseCode: payload.readUInt16(),
      }
      const leaderReference = payload.readUInt16()
      const replicasReferences = this.readReplicasReferencesFrom(response.payload)
      const leader = brokers?.find((b) => b.reference === leaderReference)
      const replicas = brokers?.filter((b) => replicasReferences.includes(b.reference))
      this.streamInfos.push({ ...streamInfo, leader, replicas })
    }
  }

  private readReplicasReferencesFrom(payload: DataReader) {
    const replicasReferences: number[] = []
    const howMany = payload.readInt32()
    for (let index = 0; index < howMany; index++) {
      const reference = payload.readUInt16()
      replicasReferences.push(reference)
    }

    return replicasReferences
  }

  get ok(): boolean {
    return true
  }
}
