# Release Notes - 2.0.0 (2026-05-01)

## Highlights
- Outbox metrics hooks with Micrometer support.
- Dead-letter replay APIs (by limit, time range, or IDs).
- Event schema versioning persisted in the outbox.

## Changes
- Added `OutboxMetricsRecorder` SPI with a default no-op implementation.
- Added `MicrometerOutboxMetricsRecorder` with counters:
  - `eventia.outbox.publish.success`
  - `eventia.outbox.publish.failure`
  - `eventia.outbox.dead_letter`
- Added replay helpers in `OutboxPublisher`:
  - `replayDeadLettered(limit)`
  - `replayDeadLetteredBetween(from, to, limit)`
  - `replayDeadLetteredByIds(ids)`
- Added `schemaVersion` to `Event` and persisted it in `OutboxEvent`.

