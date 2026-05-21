import { flow } from "../src/builder";
import type { Step } from "../src/types";

const _f = flow({
  id: "bench-50",
  input: {
    "~standard": {
      version: 1 as const,
      vendor: "bench",
      validate: (v: unknown) => ({ value: v as Record<string, never> }),
    },
  },
  build: (b) => {
    const steps: Record<string, Step<{ v: number }>> = {};
    let prev: Step<{ v: number }> | undefined;
    for (let i = 0; i < 50; i++) {
      const key = `s${String(i).padStart(2, "0")}`;
      const upstream = prev;
      const step = b.task({
        ...(upstream !== undefined ? { needs: { prev: upstream } } : {}),
        run: async () => ({ v: i }),
      });
      steps[key] = step;
      prev = step;
    }
    return steps as Record<`s${string}`, Step<{ v: number }>>;
  },
});
