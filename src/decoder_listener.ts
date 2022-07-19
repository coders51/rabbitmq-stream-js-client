import { Response } from "./responses/response"

export interface DecoderListenerFunc {
  (data: Response): void
}
