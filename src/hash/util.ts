export const imul32 = (a: number, b: number): number => {
  const aHi = (a >>> 16) & 0xffff
  const aLo = a & 0xffff
  const bHi = (b >>> 16) & 0xffff
  const bLo = b & 0xffff
  // the shift by 0 fixes the sign on the high part
  return aLo * bLo + (((aHi * bLo + aLo * bHi) << 16) >>> 0)
}

export const rotl32 = (x: number, r: number): number => {
  const rMod = r % 32
  return ((x & ((1 << (32 - rMod)) - 1)) << rMod) | (x >>> (32 - rMod))
}

export const fMix32 = (hi: number): number => {
  let h = hi
  h ^= h >>> 16
  h = imul32(h, 0x85ebca6b)
  h ^= h >>> 13
  h = imul32(h, 0xc2b2ae35)
  h ^= h >>> 16

  return h
}

export const stringToBuffer = (str: string): Buffer => {
  return typeof str === "string" ? Buffer.from(str) : Buffer.from(String(str))
}
