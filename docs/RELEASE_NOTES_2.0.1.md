# Release Notes - 2.0.1 (2026-05-02)

## Highlights
- Fixed domain event handler discovery to scan annotated methods correctly.
- Improved StreamBridge safety with bounded backpressure handling and safe publishing.
- Added aggregate and StreamBridge test coverage.

## Changes
- Aggregate rebuilds `DomainEvent` instances with stream metadata during `raiseDomainEvent`.
- StreamBridge uses bounded buffering with drop-oldest backpressure strategy.
- New unit and integration tests for aggregate handler wiring.
- StreamBridge integration tests for hot/shared behavior.

