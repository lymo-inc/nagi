import { describe, it } from "vitest";
import { InMemoryStore } from "../memory";
import { type StoreContractHarness, storeContract } from "../testing";
import type { Tx } from "../types";

const harness: StoreContractHarness = {
  async makeStore({ leaseMs }) {
    return new InMemoryStore({ leaseMs });
  },
  withTx: (_store, body) => body({} as Tx),
};

describe("Store contract — InMemoryStore", () => {
  for (const c of storeContract) {
    it(c.name, () => c.run(harness));
  }
});
