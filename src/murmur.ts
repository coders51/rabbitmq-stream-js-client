export const murmurHash =
  (seed: number) =>
  (input: string): number => {
    const c1 = 0xcc9e2d51
    const c2 = 0x1b873593
    const r1 = 15
    const r2 = 13
    const m = 5
    const n = 0xe6546b64

    let hash = seed

    const mix = (inputHash: number, mixingK: number): number => {
      let h = inputHash
      let k = mixingK
      k *= c1
      k = (k << r1) | (k >>> (32 - r1))
      k *= c2

      h ^= k
      h = (h << r2) | (h >>> (32 - r2))
      h = h * m + n

      return h >>> 0
    }

    let length = input.length
    let currentIndex = 0

    let k1 = 0

    const adjustK1 = (remainingLength: number) => {
      switch (remainingLength) {
        case 3:
          k1 ^= input.charCodeAt(currentIndex + 2) << 16
          break
        case 2:
          k1 ^= input.charCodeAt(currentIndex + 1) << 8
          break
        case 1:
          k1 ^= input.charCodeAt(currentIndex)
          k1 = k1 * c1
          k1 = (k1 << r1) | (k1 >>> (32 - r1))
          k1 = k1 * c2
          break
      }
    }

    while (length > 0) {
      if (length >= 4) {
        const k =
          input.charCodeAt(currentIndex++) |
          (input.charCodeAt(currentIndex++) << 8) |
          (input.charCodeAt(currentIndex++) << 16) |
          (input.charCodeAt(currentIndex++) << 24)

        hash = mix(hash, k)
        length -= 4
      } else {
        adjustK1(length)
        if (length === 1) {
          hash ^= k1
        }
        length -= 1
      }
    }

    hash ^= input.length
    hash ^= hash >>> 16
    hash = (hash * 0x85ebca6b) >>> 0
    hash ^= hash >>> 13
    hash = (hash * 0xc2b2ae35) >>> 0
    hash ^= hash >>> 16

    return hash >>> 0
  }
