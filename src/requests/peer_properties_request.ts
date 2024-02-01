/* eslint-disable no-param-reassign */

import { PeerPropertiesResponse } from "../responses/peer_properties_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export const PROPERTIES = {
  product: "RabbitMQ Stream",
  version: "0.3.0",
  platform: "javascript",
  copyright: "Copyright (c) 2020-2024 Coders51 srl",
  information: "Licensed under the Apache 2.0 and MPL 2.0 licenses. See https://www.rabbitmq.com/",
  connection_name: "Unknown",
}

export class PeerPropertiesRequest extends AbstractRequest {
  static readonly Key = 0x11
  static readonly Version = 1
  readonly key = PeerPropertiesRequest.Key
  readonly responseKey = PeerPropertiesResponse.key
  private readonly _properties: { key: string; value: string }[] = []

  constructor(properties: Record<string, string> = PROPERTIES) {
    super()
    this._properties = Object.keys(properties).map((key) => ({ key, value: properties[key] }))
  }

  protected writeContent(writer: DataWriter) {
    writer.writeUInt32(this._properties.length)
    this._properties.forEach(({ key, value }) => {
      writer.writeString(key)
      writer.writeString(value)
    })
  }
}
