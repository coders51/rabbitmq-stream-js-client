import { Response } from "./response"

export interface DecoderListener {
  responseReceived: (data: Response) => void
}
