// @nagi-js/otel — OpenTelemetry hooks adapter.
//
// A `FlowHooks` value that fans Nagi lifecycle events into OTel spans.
// Peer-depends on `@opentelemetry/api` only; the host application wires the
// SDK (NodeSDK, WebSDK, Workers SDK) and exporter.
//
// Wiring:
//   import { nagi } from "@nagi-js/core";
//   import { otelHooks } from "@nagi-js/otel";
//
//   const wf = nagi({
//     store: ...,
//     queue: ...,
//     flows: [...],
//     hooks: otelHooks({
//       defaultAttributes: { "deployment.environment": "production" },
//     }),
//   });
//
// Combine with your own hooks via `composeHooks`:
//
//   import { composeHooks, otelHooks } from "@nagi-js/otel";
//
//   const wf = nagi({
//     ...,
//     hooks: composeHooks(otelHooks(), myLoggerHooks),
//   });
//
// Inside a user handler, reach the active step span via `getStepSpan(ctx)`:
//
//   import { getStepSpan } from "@nagi-js/otel";
//
//   b.task({
//     run: async (ctx) => {
//       getStepSpan(ctx)?.setAttribute("billing.customer_id", ctx.input.customerId);
//       ...
//     },
//   });

export { composeHooks } from "./compose";
export { getStepSpan } from "./context";
export { otelHooks } from "./hooks";
export type { OtelHooksOpts } from "./hooks";
