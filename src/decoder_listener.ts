import { Response } from "./responses/response"

export interface DecoderListener {
  responseReceived: (data: Response) => void
}
