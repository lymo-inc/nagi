// UUIDv7 generator per RFC 9562 § 5.7.
// Time-ordered: 48-bit Unix milliseconds, 4-bit version (7), 12-bit
// sub-millisecond random, 2-bit variant (10), 62-bit random tail. Lex-sortable
// at millisecond resolution; collisions within the same ms are decided by the
// 74 random bits.
//
// Edge-safe: uses `crypto.getRandomValues` only — no node:crypto, no Buffer.

const HEX = "0123456789abcdef";

function toHex(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.length; i++) {
    const b = bytes[i] as number;
    out += HEX[b >> 4];
    out += HEX[b & 0x0f];
  }
  return out;
}

/**
 * Generate a UUIDv7 string in canonical 8-4-4-4-12 hex form.
 *
 *   `tttttttt-tttt-7rrr-vrrr-rrrrrrrrrrrr`
 *
 * where `t` is the millisecond timestamp, `7` is the version nibble, `v` is
 * the variant nibble (high two bits `10`), and `r` is random.
 */
export function uuidv7(now: number = Date.now()): string {
  const bytes = new Uint8Array(16);

  // 48-bit timestamp in big-endian, ms since unix epoch.
  // Math.floor required because Date.now() is already an integer but we want
  // to be explicit and tolerate fractional millisecond inputs from tests.
  const ts = Math.floor(now);
  // JS bitwise ops are 32-bit, so split the 48-bit value into high-16 / low-32.
  const hi = Math.floor(ts / 0x1_0000_0000);
  const lo = ts >>> 0;
  bytes[0] = (hi >>> 8) & 0xff;
  bytes[1] = hi & 0xff;
  bytes[2] = (lo >>> 24) & 0xff;
  bytes[3] = (lo >>> 16) & 0xff;
  bytes[4] = (lo >>> 8) & 0xff;
  bytes[5] = lo & 0xff;

  // 10 random bytes for the rest; we overlay the version/variant nibbles after.
  const rand = new Uint8Array(10);
  crypto.getRandomValues(rand);
  for (let i = 0; i < 10; i++) {
    bytes[6 + i] = rand[i] as number;
  }

  // Version 7 in the high nibble of byte 6.
  bytes[6] = ((bytes[6] as number) & 0x0f) | 0x70;
  // Variant 10xx in the high two bits of byte 8.
  bytes[8] = ((bytes[8] as number) & 0x3f) | 0x80;

  const hex = toHex(bytes);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
