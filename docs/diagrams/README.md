# Architecture Diagrams

These diagrams explain cross-component protocols whose correctness depends on
ordering, authority, or failure handling.

- [Provisioning claim sequence](provisioning-claim-sequence.md) — interactions
  between the queue node, metadata service, PostgreSQL, and local WAL storage.
- [Provisioning claim decision flow](provisioning-claim-flow.md) — decisions in
  one reconciliation cycle, including failures, lease expiry, and takeover.
