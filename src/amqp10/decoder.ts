export const FormatCodeType = {
  MessageHeader: 0x70,
  MessageAnnotations: 0x72,
  MessageProperties: 0x73,
  ApplicationProperties: 0x74,
  ApplicationData: 0x75,
  AmqpValue: 0x77,
  Size: 3,
} as const

export const FormatCode = {
  Described: 0x00,
  Vbin8: 0xa0,
  Str8: 0xa1,
  Sym8: 0xa3,
  Vbin32: 0xb0,
  Str32: 0xb1,
  Sym32: 0xb3,
  List32: 0xd0,
  Map32: 0xd1,
  Null: 0x40,
  SmallUlong: 0x53,
  Uint: 0x70,
  Int: 0x71,
  Timestamp: 0x83,
} as const

export const PropertySizeDescription =
  3 + // sizeOf DescribedFormatCode.Size (3 byte)
  1 + // sizeOf FormatCode.List32 (byte)
  4 + // sizeOf field numbers (uint)
  4 // sizeof propertySize (uint)
