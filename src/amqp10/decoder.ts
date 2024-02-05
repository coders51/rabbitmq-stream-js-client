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
  List0: 0x45,
  List8: 0xc0,
  List32: 0xd0,
  Map8: 0xc1,
  Map32: 0xd1,
  Null: 0x40,
  ULong0: 0x44,
  Ubyte: 0x50,
  SmallUlong: 0x53,
  ULong: 0x80,
  Uint: 0x70,
  Uint0: 0x43,
  Int: 0x71,
  SmallUint: 0x52,
  SmallInt: 0x54,
  Timestamp: 0x83,
  Bool: 0x56,
  BoolTrue: 0x41,
  BoolFalse: 0x42,
} as const

export const PropertySizeDescription =
  3 + // sizeOf DescribedFormatCode.Size (3 byte)
  1 + // sizeOf FormatCode.List32 (byte)
  4 + // sizeOf field numbers (uint)
  4 // sizeof propertySize (uint)
