// Benchmark fixture: 50-step `b.step()` chain. Used to measure tsc compile
// time for RFC 0002. Not exported and not part of any test run.
import { flow } from "../src/builder";

const _f = flow({
  id: "bench-50",
  input: {
    "~standard": {
      version: 1 as const,
      vendor: "bench",
      validate: (v: unknown) => ({ value: v as Record<string, never> }),
    },
  },
  build: (b) =>
    b
      .step("s00", { run: async () => ({ v: 0 }) })
      .step("s01", { needs: ["s00"], run: async () => ({ v: 1 }) })
      .step("s02", { needs: ["s01"], run: async () => ({ v: 2 }) })
      .step("s03", { needs: ["s02"], run: async () => ({ v: 3 }) })
      .step("s04", { needs: ["s03"], run: async () => ({ v: 4 }) })
      .step("s05", { needs: ["s04"], run: async () => ({ v: 5 }) })
      .step("s06", { needs: ["s05"], run: async () => ({ v: 6 }) })
      .step("s07", { needs: ["s06"], run: async () => ({ v: 7 }) })
      .step("s08", { needs: ["s07"], run: async () => ({ v: 8 }) })
      .step("s09", { needs: ["s08"], run: async () => ({ v: 9 }) })
      .step("s10", { needs: ["s09"], run: async () => ({ v: 10 }) })
      .step("s11", { needs: ["s10"], run: async () => ({ v: 11 }) })
      .step("s12", { needs: ["s11"], run: async () => ({ v: 12 }) })
      .step("s13", { needs: ["s12"], run: async () => ({ v: 13 }) })
      .step("s14", { needs: ["s13"], run: async () => ({ v: 14 }) })
      .step("s15", { needs: ["s14"], run: async () => ({ v: 15 }) })
      .step("s16", { needs: ["s15"], run: async () => ({ v: 16 }) })
      .step("s17", { needs: ["s16"], run: async () => ({ v: 17 }) })
      .step("s18", { needs: ["s17"], run: async () => ({ v: 18 }) })
      .step("s19", { needs: ["s18"], run: async () => ({ v: 19 }) })
      .step("s20", { needs: ["s19"], run: async () => ({ v: 20 }) })
      .step("s21", { needs: ["s20"], run: async () => ({ v: 21 }) })
      .step("s22", { needs: ["s21"], run: async () => ({ v: 22 }) })
      .step("s23", { needs: ["s22"], run: async () => ({ v: 23 }) })
      .step("s24", { needs: ["s23"], run: async () => ({ v: 24 }) })
      .step("s25", { needs: ["s24"], run: async () => ({ v: 25 }) })
      .step("s26", { needs: ["s25"], run: async () => ({ v: 26 }) })
      .step("s27", { needs: ["s26"], run: async () => ({ v: 27 }) })
      .step("s28", { needs: ["s27"], run: async () => ({ v: 28 }) })
      .step("s29", { needs: ["s28"], run: async () => ({ v: 29 }) })
      .step("s30", { needs: ["s29"], run: async () => ({ v: 30 }) })
      .step("s31", { needs: ["s30"], run: async () => ({ v: 31 }) })
      .step("s32", { needs: ["s31"], run: async () => ({ v: 32 }) })
      .step("s33", { needs: ["s32"], run: async () => ({ v: 33 }) })
      .step("s34", { needs: ["s33"], run: async () => ({ v: 34 }) })
      .step("s35", { needs: ["s34"], run: async () => ({ v: 35 }) })
      .step("s36", { needs: ["s35"], run: async () => ({ v: 36 }) })
      .step("s37", { needs: ["s36"], run: async () => ({ v: 37 }) })
      .step("s38", { needs: ["s37"], run: async () => ({ v: 38 }) })
      .step("s39", { needs: ["s38"], run: async () => ({ v: 39 }) })
      .step("s40", { needs: ["s39"], run: async () => ({ v: 40 }) })
      .step("s41", { needs: ["s40"], run: async () => ({ v: 41 }) })
      .step("s42", { needs: ["s41"], run: async () => ({ v: 42 }) })
      .step("s43", { needs: ["s42"], run: async () => ({ v: 43 }) })
      .step("s44", { needs: ["s43"], run: async () => ({ v: 44 }) })
      .step("s45", { needs: ["s44"], run: async () => ({ v: 45 }) })
      .step("s46", { needs: ["s45"], run: async () => ({ v: 46 }) })
      .step("s47", { needs: ["s46"], run: async () => ({ v: 47 }) })
      .step("s48", { needs: ["s47"], run: async () => ({ v: 48 }) })
      .step("s49", { needs: ["s48"], run: async () => ({ v: 49 }) }),
});
