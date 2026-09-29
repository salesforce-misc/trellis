# Stage 1 — Logical replication into the staging area (replaced)

← [Overview](README.md)

Trellis no longer captures changes through logical replication. Since #622
C5, capture triggers write each change's ring rows in the writer's own
transaction: see [Stage 1 — Capture by triggers](01-capture-by-triggers.md).

The intake code (the replication stream, the slot and the publication) is
still compiled but never started. #622 C8 deletes it, and this page with it.
