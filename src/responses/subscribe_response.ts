import { AbstractResponse } from "./abstract_response"

export class SubscribeResponse extends AbstractResponse {
  static key = 0x8007
  static MinVersion = 1
  static MaxVersion = 1
}
