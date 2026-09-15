import { describe, it } from "vitest";
import { InMemoryStore } from "../memory";
import { type StoreContractHarness, storeContract } from "../testing";

const harness: StoreContractHarness = {
  async makeStore({ leaseMs }) {
    return new InMemoryStore({ leaseMs });
  },
};

describe("Store contract — InMemoryStore", () => {
  for (const c of storeContract) {
    it(c.name, () => c.run(harness));
  }
});
