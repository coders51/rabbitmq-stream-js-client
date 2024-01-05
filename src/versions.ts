import { Logger } from "./logger"
import * as requests from "./requests/requests"
import * as responses from "./responses/responses"

export type Version = { key: number; minVersion: number; maxVersion: number }
type Key = number
type MappedVersions = Map<Key, Version>

const supportedRequests = [
  requests.CloseRequest,
  requests.CreateStreamRequest,
  requests.CreditRequest,
  requests.DeclarePublisherRequest,
  requests.DeletePublisherRequest,
  requests.DeleteStreamRequest,
  requests.ExchangeCommandVersionsRequest,
  requests.HeartbeatRequest,
  requests.MetadataRequest,
  requests.MetadataUpdateRequest,
  requests.OpenRequest,
  requests.PeerPropertiesRequest,
  requests.PublishRequest,
  requests.QueryOffsetRequest,
  requests.QueryPublisherRequest,
  requests.SaslAuthenticateRequest,
  requests.SaslHandshakeRequest,
  requests.StoreOffsetRequest,
  requests.StreamStatsRequest,
  requests.SubscribeRequest,
  requests.TuneRequest,
  requests.UnsubscribeRequest,
]

const supportedResponses = [responses.DeliverResponse, responses.PublishConfirmResponse, responses.PublishErrorResponse]

function getClientSupportedVersions() {
  const result: Version[] = supportedRequests.map((requestClass) => ({
    key: requestClass.Key,
    minVersion: requestClass.MinVersion,
    maxVersion: requestClass.MaxVersion,
  }))

  result.push(
    ...supportedResponses.map((responseClass) => ({
      key: responseClass.key,
      minVersion: responseClass.MinVersion,
      maxVersion: responseClass.MaxVersion,
    }))
  )

  return result
}

export const clientSupportedVersions: Version[] = getClientSupportedVersions()

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
