import { Logger } from "./logger"
import { CloseRequest } from "./requests/close_request"
import { CreateStreamRequest } from "./requests/create_stream_request"
import { CreditRequest } from "./requests/credit_request"
import { DeclarePublisherRequest } from "./requests/declare_publisher_request"
import { DeletePublisherRequest } from "./requests/delete_publisher_request"
import { DeleteStreamRequest } from "./requests/delete_stream_request"
import { ExchangeCommandVersionsRequest } from "./requests/exchange_command_versions_request"
import { HeartbeatRequest } from "./requests/heartbeat_request"
import { MetadataRequest } from "./requests/metadata_request"
import { MetadataUpdateRequest } from "./requests/metadata_update_request"
import { OpenRequest } from "./requests/open_request"
import { PeerPropertiesRequest } from "./requests/peer_properties_request"
import { PublishRequest } from "./requests/publish_request"
import { QueryOffsetRequest } from "./requests/query_offset_request"
import { QueryPublisherRequest } from "./requests/query_publisher_request"
import { SaslAuthenticateRequest } from "./requests/sasl_authenticate_request"
import { SaslHandshakeRequest } from "./requests/sasl_handshake_request"
import { StoreOffsetRequest } from "./requests/store_offset_request"
import { StreamStatsRequest } from "./requests/stream_stats_request"
import { SubEntryBatchPublishRequest } from "./requests/sub_entry_batch_publish_request"
import { SubscribeRequest } from "./requests/subscribe_request"
import { TuneRequest } from "./requests/tune_request"
import { UnsubscribeRequest } from "./requests/unsubscribe_request"

export type Version = { key: number; minVersion: number; maxVersion: number }
type Key = number
type MappedVersions = Map<Key, Version>

const supportedRequests = [
  CloseRequest,
  CreateStreamRequest,
  CreditRequest,
  DeclarePublisherRequest,
  DeletePublisherRequest,
  DeleteStreamRequest,
  ExchangeCommandVersionsRequest,
  HeartbeatRequest,
  MetadataRequest,
  MetadataUpdateRequest,
  OpenRequest,
  PeerPropertiesRequest,
  PublishRequest,
  QueryOffsetRequest,
  QueryPublisherRequest,
  SaslAuthenticateRequest,
  SaslHandshakeRequest,
  StoreOffsetRequest,
  StreamStatsRequest,
  SubEntryBatchPublishRequest,
  SubscribeRequest,
  TuneRequest,
  UnsubscribeRequest,
]

export const clientSupportedVersions: Version[] = supportedRequests.map((requestClass) => ({
  key: requestClass.Key,
  minVersion: requestClass.MinVersion,
  maxVersion: requestClass.MaxVersion,
}))

function indexVersions(versions: Version[]) {
  const result = new Map<number, Version>()
  versions.forEach((v) => result.set(v.key, v))

  return result
}

function checkVersion(
  key: number,
  minVersion: number,
  maxVersion: number,
  compared: Version | undefined,
  logger: Logger
) {
  if (minVersion > 1 && compared === undefined) {
    logger.error(`For message key ${key.toString(16)} version mismatch between client and server`)
    return false
  }

  if (compared === undefined) return true

  if (minVersion > compared.maxVersion || compared.minVersion > maxVersion) {
    logger.error(`For message key ${key.toString(16)} version mismatch between client and server`)
    return false
  }
  return true
}

function checkVersions(side1Versions: MappedVersions, side2Versions: MappedVersions, logger: Logger) {
  let result = true
  for (const e of side1Versions.entries()) {
    const [key, side1Version] = e
    const side2Version = side2Versions.get(key)
    result = result && checkVersion(key, side1Version.minVersion, side1Version.maxVersion, side2Version, logger)
  }

  return result
}

export function checkServerDeclaredVersions(serverDeclaredVersions: Version[], logger: Logger) {
  const indexedClientVersions = indexVersions(clientSupportedVersions)
  const indexedServerVersions = indexVersions(serverDeclaredVersions)
  return (
    checkVersions(indexedClientVersions, indexedServerVersions, logger) &&
    checkVersions(indexedServerVersions, indexedClientVersions, logger)
  )
}
