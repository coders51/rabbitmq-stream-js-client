export interface Locator {
  queryPartitions: (params: { superStream: string }) => Promise<string[]>
  routeQuery: (params: { routingKey: string; superStream: string }) => Promise<string[]>
}
