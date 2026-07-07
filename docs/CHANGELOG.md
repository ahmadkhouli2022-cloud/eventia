# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [2.1.8] - 2026-07-07
- Remove unused `IDType` generic parameter from `Aggregate`.

## [2.1.7] - 2026-06-14
- Fix `@HandleDomainEvent` discovery on `@Configuration` `@Bean` factory methods.
- Fix `AggregateIntegrationTest` Spring Cloud Stream binding and test isolation.

## [2.0.1] - 2026-05-02
- Fix domain event handler discovery and invocation.
- Harden StreamBridge backpressure handling and publishing safety.
- Add aggregate and stream bridge tests.
- Release notes: `docs/RELEASE_NOTES_2.0.1.md`

## [2.0.0] - 2026-05-01
- Phase 2 updates: metrics hooks, replay APIs, schema versioning.
- Release notes: `docs/RELEASE_NOTES_2.0.0.md`
