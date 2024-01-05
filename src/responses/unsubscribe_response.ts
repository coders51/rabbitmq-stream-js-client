import { AbstractResponse } from "./abstract_response"

export class UnsubscribeResponse extends AbstractResponse {
  static key = 0x800c
  static MinVersion = 1
  static MaxVersion = 1
}
