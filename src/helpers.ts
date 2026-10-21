import { Client } from "./client"
import { Offset } from "./requests/subscribe_request"
import { isStreamResponseError } from "./stream_response_error"
import { ResponseCode } from "./util"

/**
 * Resolves the starting offset for a consumer by checking the server for a stored offset.
 * If no offset is stored, the provided default offset is used.
 *
 * `stream` must be the name of a partition stream (e.g. `my-super-stream-0`), not the super stream name:
 * offsets are stored per partition.
 *
 * For a super stream consumer, pass it to `consumerUpdateListener` so each partition resolves its own offset:
 * `consumerUpdateListener: (ref, partition) => resolveStartOffset(client, Offset.first(), ref, partition)`
 */
export async function resolveStartOffset(
  client: Client,
  defaultOffset: Offset,
  consumerRef: string,
  stream: string
): Promise<Offset> {
  try {
    const stored = await client.queryOffset({ reference: consumerRef, stream })
    return Offset.offset(stored + 1n)
  } catch (error) {
    if (isStreamResponseError(error) && error.code === ResponseCode.NoOffset) {
      return defaultOffset
    }
    throw error
  }
}
