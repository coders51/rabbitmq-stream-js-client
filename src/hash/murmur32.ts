// Original from https://github.com/flagUpDown/murmurhash-node, licensed ISC
// Linted, cleaned up and properly typed for this project

import { fMix32, imul32, rotl32, stringToBuffer } from "./util"

const MURMUR_HASH_C1 = 0xcc9e2d51
const MURMUR_HASH_C2 = 0x1b873593

export const murmur32 = (key: string): number => {
  const seed = 104729 //  must be the same to all the clients to be compatible
  const bKey = stringToBuffer(key)

  const len = bKey.length
  let remainder = len & 3
  const bytes = len - remainder

  let h1 = seed

  let i = 0
  while (i < bytes) {
    let k1 = bKey[i++] | (bKey[i++] << 8) | (bKey[i++] << 16) | (bKey[i++] << 24)

    k1 = imul32(k1, MURMUR_HASH_C1)
    k1 = ((k1 & 0x1ffff) << 15) | (k1 >>> 17)
    k1 = imul32(k1, MURMUR_HASH_C2)

    h1 ^= k1
    h1 = ((h1 & 0x7ffff) << 13) | (h1 >>> 19)
    h1 = (imul32(h1, 5) >>> 0) + 0xe6546b64
  }

  let k1 = 0
  while (remainder > 0) {
    switch (remainder) {
      case 3:
        k1 ^= bKey[i + 2] << 16
        break
      case 2:
        k1 ^= bKey[i + 1] << 8
        break
      case 1:
        k1 ^= bKey[i]

        k1 = imul32(k1, MURMUR_HASH_C1)
        k1 = rotl32(k1, 15)
        k1 = imul32(k1, MURMUR_HASH_C2)
        h1 ^= k1
        break
    }
    remainder -= 1
  }
  h1 ^= len

  return fMix32(h1) >>> 0
}
