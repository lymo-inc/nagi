---
"@nagi-js/core": patch
---

Internal engine cleanup — no public API change, behavior preserved.

- **Extract `nextTransition(flow, runState)` from the `advance` loop** (RFC 0011). The engine's per-tick decision is now a pure, exhaustively-typed `Transition` union (`promote-match | complete | fail | dispatch | skip | settled | waiting`) in `scheduler.ts`, and `advance` is a thin executor switch in `dispatch.ts`. Double-finalize is now structurally prevented (`complete`/`fail` are only emitted when the flow is done and not already terminal). Adds 9 direct `nextTransition` unit tests.
- **Consolidate match/subflow step finalization** into shared `markStepComplete` / `markStepFail` helpers (was duplicated across match promotion and subflow wake).
- **Reuse `serializeError()` in runtime** instead of hand-building `SerializedError`; dedupe a repeated cancel-error literal.
- **`instanceof NagiAbortError`** for our own abort (keeps the foreign-`AbortError` name check for handler-thrown DOMExceptions).
- **Drop dead defensive guards** on `def.needs` (the type already guarantees `Step` refs).
- **Split pre-walk vs finalized match arms**: the `_nested` ghost field is gone from `MatchArmDef`; pre-walk concerns live on new builder-only `PendingMatchArm` / `PendingMatchDef`.
- **`DispatchDeps.lookupFlow` / `.startChildRun` are now required** (always wired by the runtime), removing a runtime "missing dep" guard from subflow dispatch.
