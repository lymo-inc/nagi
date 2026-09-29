---
"@nagi-js/postgres": patch
"@nagi-js/pgmq": patch
---

`@nagi-js/core` is now a peer dependency of every adapter, pinned to the exact core version it was released with, instead of a private dependency.

Install core alongside the adapter (`pnpm add @nagi-js/core @nagi-js/postgres`). Most apps already do, since they import `nagi` from it.

Before this, upgrading `@nagi-js/core` alone installed a second copy of core inside the adapter. Errors thrown by the store were then not `instanceof` the engine's classes, and the store applied the older core's fact rules. A mismatched pair now fails at install time (npm `ERESOLVE`, pnpm unmet-peer warning). Upgrade core and adapters together.
