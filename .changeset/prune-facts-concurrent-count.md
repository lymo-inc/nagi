---
"@nagi-js/postgres": patch
---

`postgresStore.pruneFacts` over-reported `runsPruned` when two pruners ran
concurrently: under READ COMMITTED the second pruner's victim SELECT could
return runs whose facts the first had already deleted, and each was counted as
pruned. `runsPruned` now counts only runs whose facts the call actually
removed, and the batch loop stops on no candidates rather than on no deletions.
