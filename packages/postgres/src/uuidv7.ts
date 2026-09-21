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

// RFC 9562 rand_a is 12 bits. Used as a per-millisecond counter (method 2) so
// ids minted inside one millisecond still sort in creation order. Seeded in the
// low half, leaving >= 2048 increments of headroom before it saturates.
const COUNTER_MAX = 0xfff;
const COUNTER_SEED_MAX = 0x800;

let lastMs = -1;
let counter = 0;

function seedCounter(): number {
  const b = new Uint8Array(2);
  crypto.getRandomValues(b);
  return (((b[0] as number) << 8) | (b[1] as number)) % COUNTER_SEED_MAX;
}

// Sortable, collision-resistant id. Ordering matters beyond neatness: the fact
// table is read back with ORDER BY fact_id, so fact_id order IS append order,
// and foldRun replays a run from it. A purely random rand_a — what this used to
// have — let two facts written in the same millisecond come back reversed.
export function uuidv7(now: number = Date.now()): string {
  const bytes = new Uint8Array(16);

  const ts = Math.floor(now);
  if (ts === lastMs) {
    // Saturate rather than wrap: wrapping would sort this id BEFORE the one it
    // follows, which is the exact failure the counter exists to prevent.
    if (counter < COUNTER_MAX) counter += 1;
  } else {
    lastMs = ts;
    counter = seedCounter();
  }

  const hi = Math.floor(ts / 0x1_0000_0000);
  const lo = ts >>> 0;
  bytes[0] = (hi >>> 8) & 0xff;
  bytes[1] = hi & 0xff;
  bytes[2] = (lo >>> 24) & 0xff;
  bytes[3] = (lo >>> 16) & 0xff;
  bytes[4] = (lo >>> 8) & 0xff;
  bytes[5] = lo & 0xff;

  // rand_a carries the counter; rand_b stays fully random, so uniqueness never
  // depends on the counter having headroom.
  const rand = new Uint8Array(8);
  crypto.getRandomValues(rand);
  for (let i = 0; i < 8; i++) {
    bytes[8 + i] = rand[i] as number;
  }

  bytes[6] = 0x70 | ((counter >>> 8) & 0x0f);
  bytes[7] = counter & 0xff;
  bytes[8] = ((bytes[8] as number) & 0x3f) | 0x80;

  const hex = toHex(bytes);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
