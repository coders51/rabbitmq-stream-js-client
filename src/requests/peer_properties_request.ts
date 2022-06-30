/* eslint-disable no-param-reassign */

import { PeerPropertiesResponse } from "../responses/peer_properties_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./sasl_authenticate_request"

export const PROPERTIES = {
  product: "RabbitMQ Stream",
  version: "0.0.1",
  platform: "javascript",
  copyright: "Copyright (c) 2020-2021 Coders51 srl",
  information: "Licensed under the Apache 2.0 and MPL 2.0 licenses. See https://www.rabbitmq.com/",
  connection_name: "Unknown",
}

export class PeerPropertiesRequest extends AbstractRequest {
  readonly key = 0x11
  readonly responseKey = PeerPropertiesResponse.key
  private readonly _properties: { key: string; value: string }[] = []

  constructor(properties: Record<string, string> = PROPERTIES) {
    super()
    this._properties = Object.keys(properties).map((key) => ({ key, value: properties[key] }))
  }

  protected writeContent(b: DataWriter) {
    b.writeUInt32(this._properties.length)
    this._properties.forEach(({ key, value }) => {
      b.writeString(key)
      b.writeString(value)
    })
  }
}
