---
"@nagi-js/postgres": patch
---

`migrate()` serializes concurrent calls on a per-schema advisory lock, so every replica can run it at startup.
