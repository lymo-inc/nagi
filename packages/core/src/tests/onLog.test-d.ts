import { describe, expectTypeOf, it } from "vitest";
import type { NagiConfig } from "../runtime";
import { nagi } from "../runtime";
import type {
  LogEntry,
  Logger,
  LogLevel,
  Queue,
  StepCtx,
  Store,
} from "../types";

// Stubs to construct `nagi(...)` in type-level tests — bodies never run.
declare const store: Store;
declare const queue: Queue;
declare const methodLogger: Logger;

describe("NagiConfig.onLog — exact shape", () => {
  it("onLog is exactly ((entry: LogEntry) => void) | undefined", () => {
    expectTypeOf<NagiConfig["onLog"]>().toEqualTypeOf<
      ((entry: LogEntry) => void) | undefined
    >();
  });

  it("onLog's first parameter is LogEntry", () => {
    expectTypeOf<NonNullable<NagiConfig["onLog"]>>()
      .parameter(0)
      .toEqualTypeOf<LogEntry>();
  });

  it("onLog returns void", () => {
    expectTypeOf<NonNullable<NagiConfig["onLog"]>>().returns.toBeVoid();
  });

  it("onLog is an optional field (key may be omitted)", () => {
    // The field is optional: a config object with no onLog is assignable.
    expectTypeOf<{
      flows: NagiConfig["flows"];
      store: Store;
      queue: Queue;
    }>().toMatchTypeOf<Omit<NagiConfig, "onLog"> & Partial<NagiConfig>>();
    // `undefined` is assignable to the property type (optional).
    expectTypeOf<undefined>().toMatchTypeOf<NagiConfig["onLog"]>();
  });
});

describe("LogEntry — field types", () => {
  it("level is the exhaustive LogLevel union, not string", () => {
    expectTypeOf<LogEntry["level"]>().toEqualTypeOf<
      "debug" | "info" | "warn" | "error"
    >();
    expectTypeOf<LogEntry["level"]>().toEqualTypeOf<LogLevel>();
    // It is NOT widened to string.
    expectTypeOf<LogEntry["level"]>().not.toEqualTypeOf<string>();
  });

  it("LogLevel itself is the closed four-member union", () => {
    expectTypeOf<LogLevel>().toEqualTypeOf<
      "debug" | "info" | "warn" | "error"
    >();
  });

  it("msg is string", () => {
    expectTypeOf<LogEntry["msg"]>().toBeString();
  });

  it("attrs is Record<string, unknown> | undefined and optional", () => {
    expectTypeOf<LogEntry["attrs"]>().toEqualTypeOf<
      Record<string, unknown> | undefined
    >();
    // Optional: a LogEntry without attrs is constructible.
    expectTypeOf<{ level: LogLevel; msg: string }>().toMatchTypeOf<LogEntry>();
  });

  it("attrs values are unknown, not any", () => {
    type V = NonNullable<LogEntry["attrs"]>[string];
    expectTypeOf<V>().toEqualTypeOf<unknown>();
    // `any` would make `unknown extends V` collapse; assert V is not any by
    // proving a value of V is not assignable to a concrete type.
    expectTypeOf<V>().not.toEqualTypeOf<string>();
  });
});

describe("LogEntry — readonly fields", () => {
  it("level / msg / attrs cannot be reassigned", () => {
    const entry: LogEntry = { level: "info", msg: "x" };
    // @ts-expect-error level is readonly
    entry.level = "warn";
    // @ts-expect-error msg is readonly
    entry.msg = "y";
    // @ts-expect-error attrs is readonly
    entry.attrs = { a: 1 };
    void entry;
  });
});

describe("Logger object is NOT assignable to onLog (the hard break proof)", () => {
  it("a method-shaped Logger is not an (entry: LogEntry) => void", () => {
    // @ts-expect-error a 4-method Logger object is not a single onLog callback
    const cb: NonNullable<NagiConfig["onLog"]> = methodLogger;
    void cb;
  });

  it("the old `logger` key is gone from NagiConfig (compile error)", () => {
    void nagi({
      flows: [],
      store,
      queue,
      // @ts-expect-error `logger` was removed in RFC 0020 — only `onLog` remains
      logger: {
        debug() {},
        info() {},
        warn() {},
        error() {},
      },
    });
  });

  it("passing `logger` to a bare NagiConfig object literal is a type error", () => {
    const cfg: NagiConfig = {
      flows: [],
      store,
      queue,
      // @ts-expect-error `logger` is not a member of NagiConfig
      logger: {} as Logger,
    };
    void cfg;
  });

  it("onLog accepts a structural (entry) => void callback", () => {
    void nagi({
      flows: [],
      store,
      queue,
      onLog: (entry) => {
        expectTypeOf(entry).toEqualTypeOf<LogEntry>();
      },
    });
  });
});

describe("StepCtx.logger — method-shaped in-step surface (O1)", () => {
  it("ctx.logger is the Logger interface", () => {
    expectTypeOf<StepCtx["logger"]>().toEqualTypeOf<Logger>();
  });

  it("ctx.logger.info takes (string, attrs?) and returns void", () => {
    expectTypeOf<StepCtx["logger"]["info"]>().parameter(0).toBeString();
    expectTypeOf<StepCtx["logger"]["info"]>()
      .parameter(1)
      .toEqualTypeOf<Record<string, unknown> | undefined>();
    expectTypeOf<StepCtx["logger"]["info"]>().returns.toBeVoid();
  });

  it("all four level methods exist and share the (string, attrs?) signature", () => {
    expectTypeOf<StepCtx["logger"]["debug"]>().parameter(0).toBeString();
    expectTypeOf<StepCtx["logger"]["warn"]>().parameter(0).toBeString();
    expectTypeOf<StepCtx["logger"]["error"]>().parameter(0).toBeString();
    expectTypeOf<StepCtx["logger"]["debug"]>().returns.toBeVoid();
    expectTypeOf<StepCtx["logger"]["warn"]>().returns.toBeVoid();
    expectTypeOf<StepCtx["logger"]["error"]>().returns.toBeVoid();
  });

  it("attrs passed to ctx.logger.info has unknown values, not any", () => {
    type Attrs = NonNullable<Parameters<StepCtx["logger"]["info"]>[1]>;
    expectTypeOf<Attrs[string]>().toEqualTypeOf<unknown>();
  });
});
