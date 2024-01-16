import { Logger } from "./logger"
import * as requests from "./requests/requests"
import * as responses from "./responses/responses"

export type Version = { key: number; minVersion: number; maxVersion: number }
type Key = number
type SimpleVersion = number
type MappedVersions = Map<Key, Version>

const supportedRequests = [
  requests.CloseRequest,
  requests.CreateStreamRequest,
  requests.CreateSuperStreamRequest,
  requests.CreditRequest,
  requests.DeclarePublisherRequest,
  requests.DeletePublisherRequest,
  requests.DeleteStreamRequest,
  requests.DeleteSuperStreamRequest,
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
  requests.RouteQuery,
  requests.PartitionsQuery,
]

const supportedResponses = [
  responses.DeliverResponse,
  responses.PublishConfirmResponse,
  responses.PublishErrorResponse,
  responses.ConsumerUpdateQuery,
]

function maybeAddMaxVersion(values: Map<Key, SimpleVersion>, key: Key, version: SimpleVersion) {
  const currentMaxValue = values.get(key)
  if (currentMaxValue === undefined || currentMaxValue < version) values.set(key, version)
}
function maybeAddMinVersion(values: Map<Key, SimpleVersion>, key: Key, version: SimpleVersion) {
  const currentMinValue = values.get(key)
  if (currentMinValue === undefined || currentMinValue > version) values.set(key, version)
}

function getClientSupportedVersions() {
  const minValues = new Map<Key, SimpleVersion>()
  const maxValues = new Map<Key, SimpleVersion>()

  supportedRequests.forEach((requestClass) => {
    maybeAddMaxVersion(maxValues, requestClass.Key, requestClass.Version)
    maybeAddMinVersion(minValues, requestClass.Key, requestClass.Version)
  })

  supportedResponses.forEach((responseClass) => {
    maybeAddMaxVersion(maxValues, responseClass.key, responseClass.Version)
    maybeAddMinVersion(minValues, responseClass.key, responseClass.Version)
  })

  const result: Version[] = []
  for (const k of minValues.keys()) {
    const minVersion = minValues.get(k)
    const maxVersion = maxValues.get(k)
    result.push({ key: k, minVersion: minVersion!, maxVersion: maxVersion! })
  }

  return result
}

export const clientSupportedVersions: Version[] = getClientSupportedVersions()

function indexVersions(versions: Version[]) {
  const result = new Map<Key, Version>()
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
