// Benchmark fixture: 15-step `b.step()` chain. Used to measure tsc compile
// time for RFC 0002. Not exported and not part of any test run.
import { flow } from "../src/builder";

const _f = flow({
  id: "bench-15",
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
      .step("s14", { needs: ["s13"], run: async () => ({ v: 14 }) }),
});
export {};
