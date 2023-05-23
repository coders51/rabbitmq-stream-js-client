import { ExchangeCommandVersionsResponse } from "../responses/exchange_versions_response"
import { AbstractRequest } from "./abstract_request"
import { DataWriter } from "./data_writer"

export const PROPERTIES = {
  product: "RabbitMQ Stream",
  version: "0.0.1",
  platform: "javascript",
  copyright: "Copyright (c) 2020-2021 Coders51 srl",
  information: "Licensed under the Apache 2.0 and MPL 2.0 licenses. See https://www.rabbitmq.com/",
  connection_name: "Unknown",
}

interface CommandInformer {
  key: number
  minVersion: number
  maxVersion: number
}

export const commandsInitiatedServerSide: CommandInformer[] = [
  {
    key: 1,
    minVersion: 1,
    maxVersion: 1,
  },
]

export class ExchangeCommandVersionsRequest extends AbstractRequest {
  readonly responseKey = ExchangeCommandVersionsResponse.key
  readonly key = 0x001b
  private readonly _properties: CommandInformer[] = []

  constructor(properties: CommandInformer[]) {
    super()
    this._properties = properties
  }

  writeContent(writer: DataWriter) {
    writer.writeUInt16(this.commandKey)
  }
}
