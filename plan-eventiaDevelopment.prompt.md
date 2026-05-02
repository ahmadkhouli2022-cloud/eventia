# Eventia Project Evaluation and Development Plan

## Evaluation Summary

### Architecture and Design
- Clear separation of concerns across `domain`, `infrastructure`, and `messages` packages. The aggregate pattern and outbox approach are established and mostly consistent.
- The outbox model is coherent, but core persistence orchestration is incomplete. `JpaDataPersistentWithEventStore` collects events but does not persist them to the outbox.
- Domain event handler registration uses reflection and application context scanning. The current approach ties domain layer to Spring infrastructure, which weakens clean boundaries.
- Reactive and websocket packages provide useful adapters but are loosely integrated and not fully documented.

### Code Engineering Quality
- Concurrency: `Aggregate.Domain` uses a `ReentrantLock` to guard event lists and versioning, which is good, but lock usage is manual and repeated. Consider simplifying or encapsulating.
- Serialization: `OutboxEvent` uses class name for deserialization, which can become a security or compatibility risk if class names change. No versioning strategy for events is present.
- Error handling: `OutboxPublisher` treats failures with retries and DLQ, but retry config is static and not externally configurable. Cleanup job is a placeholder.
- API consistency: `OutboxStore` returns `OutboxEvent findById(String)` while IDs are `UUID`; this mismatch can leak into implementations.

### Build and Release
- Maven central publishing is configured, but `settings.xml` includes credentials; these should be moved to environment/CI secrets.
- Java 21 is used in build while README says Java 17+. Align documented compatibility.
- No `src/test` is present; quality gates are weak and CI only runs `mvn package`.

### Documentation Gaps
- README promises features (metrics, monitoring) not fully present in code.
- Roadmap dates are outdated; should be refreshed for 2026.
- No explicit API docs for reactive bus, websocket, or outbox customization.

## Development Plan

### Phase 0: Hygiene and Baseline (1-2 weeks)
- Remove credentials from `settings.xml` and switch to CI secrets.
- Align Java version documentation with build (`README.md`).
- Introduce `src/test` with a minimal test harness for aggregate/outbox flows.
- Add a changelog and release checklist in `docs/`.

### Phase 1: Core Outbox Completion (2-4 weeks)
- Implement event persistence in `JpaDataPersistentWithEventStore` by injecting `OutboxStore` and saving events transactionally.
- Make retry parameters configurable via properties (maxRetries, batchSize, backoff).
- Implement outbox cleanup job with `OutboxStore.deletePublishedBefore`.
- Normalize ID types in `OutboxStore.findById` to `UUID`.

### Phase 2: Reliability and Observability (3-5 weeks)
- Add metrics (counts, latency, retry rate, DLQ size) via Micrometer or pluggable SPI.
- Add event replay utility (publish from a time range, or by ID list).
- Provide event schema versioning fields in `Event` or `OutboxEvent` with migration strategy.

### Phase 3: Integrations and Developer Experience (4-6 weeks)
- Provide official adapters: Kafka and RabbitMQ publishing via `EventPublisher` SPI.
- Expand reactive bus documentation and add example usage.
- Add a small example app under `examples/` to show end-to-end outbox flow.

## Milestone Targets
- 1.0.1: Hygiene + tests + cleanup job + doc fixes
- 1.1.0: Core outbox completion + configurable retry + metrics
- 1.2.0: Replay + event versioning + integration adapters
- 2.0.0: Expanded multi-tenant support and stronger API boundaries

## Immediate Action Items
- Decide target release: 1.0.1 patch or 1.1 minor.
- Confirm priority: reliability vs. new integrations.
- Choose first metrics stack (Micrometer or custom SPI).

