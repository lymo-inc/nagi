---
"@nagi-js/core": patch
---

`InMemoryQueue` now redelivers a read whose visibility timeout lapsed without an ack or nack, as pgmq does. Before, a message whose ack failed or whose consumer died stayed leased forever, so a test running on the in-memory pair could report a stranded run that cannot happen on Postgres + pgmq.
