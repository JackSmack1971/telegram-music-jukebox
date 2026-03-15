# Changelog

All notable changes to the **Telegram Music Jukebox** project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [0.4.0] — 2026-03-14 — Phase 4: DevOps, Testing & Documentation

### Added

- Chaos engineering harness for failure injection (Redis partition, disk full, FFmpeg crash, network latency, voice chat closure) (`603db71`)
- Recovery latency distribution, seek accuracy, and max retry exhaustion tests (`25b3d40`)
- Load tests for concurrent asyncio streams and queue throughput measurement (`d1a2733`)
- Error-scenario test suite covering Redis connection failures, disk space errors, FFmpeg crashes, and network partitions (`f4902a0`)
- Shared pytest fixtures via `conftest.py` (`361208d`)
- Application configuration module (`settings.py`) using Pydantic v2 with environment variable loading (`cb81994`)
- Systemd service unit file with dependency ordering, resource limits, and structured logging (`12adea9`)
- Redis server configuration with initial tuning (`a45fec5`)
- Redis Sentinel configuration for HA cluster failover (`e057990`)
- Multi-stage Dockerfile for production image (`22f4b49`)
- Docker Compose stack (bot, Redis, Sentinel) (`559d0da`)
- Revised `.env.example` with required fields and security annotations (`0e0e308`)
- `requirements-test.txt` for testing and coverage tooling (`326bf63`)
- `requirements-dev.txt` for code quality and documentation tools (`f796e57`)
- Expanded root README with architecture decisions, deployment guide, performance metrics, and roadmap (`6268614`)

### Changed

- Commented out optional async utility dependencies in `requirements.txt` (`733d028`)
- Added Pydantic / Pydantic Settings to core `requirements.txt` (`c227b5c`)

---

## [0.3.0] — 2026-03-14 — Phase 3: Telegram Gateway

### Added

- `TelegramGateway` implementation using aiogram 3.x with command routing (`f0edb3e`)
- `CommandBroker` for centralised command dispatching via aiogram Router (`621f6bd`)
- `/play` command handler with URL detection, yt-dlp search, and automatic playback management (`30f2eae`)
- Playback control commands: `/skip`, `/stop`, `/pause`, `/resume`, `/volume`, `/loop` (`f0b1248`)
- `/queue` command handler with inline-keyboard pagination (`5c04e29`)
- `/np` (Now Playing) command with ASCII progress bar, loop mode, and volume display (`7063f85`)
- Permission system for command access control (admin check, DJ role validation, voice chat rights) (`1326429`)
- Token-bucket message throttle middleware for per-group rate limiting (`c2d90c6`)
- Phase 2 & 3 dependency manifest (`0d6e380`)
- Phase 2 & 3 README with usage instructions and architecture notes (`ed41057`)

---

## [0.2.0] — 2026-03-14 — Phase 2: Streaming Core

### Added

- `ConcreteStreamEngine` for audio streaming to Telegram voice chats (start, stop, pause, resume, state management) (`663ffc2`)
- Global `SemaphorePool` (singleton) limiting concurrent FFmpeg streams to prevent resource exhaustion (`d7b6015`)
- `StreamWatchdog` for FFmpeg process health monitoring with heartbeat polling and recovery (`eafa9be`)
- Seek-resume recovery system for FFmpeg crashes with position tracking and exponential backoff (`e3aee0b`)
- `TmpfsOverlay` for staging actively-streamed audio files in tmpfs with capacity checks (`7f9c2b6`)
- `LRUDiskCacheManager` for audio files with a 20 GB limit and Redis-backed metadata/eviction (`9f520ad`)
- `DownloadWorker` using yt-dlp with Redlock deduplication, progress tracking, and error handling (`ef4dbc0`)
- Phase 1 README expanded with key patterns, environment variables, usage, testing, and architecture notes (`4046000`)
- Initial `requirements.txt` with foundational dependencies (`40875e8`)
- Phase 1 README stub (`c001476`)

---

## [0.1.0] — 2026-03-14 — Phase 1: Domain & Infrastructure

### Added

- Domain enumerations for jukebox state machines (`3595935`)
- Core domain models (`Track`, `PlaybackState`, `GroupSettings`, `DownloadResult`, `JukeboxError`) using Python 3.11 `dataclasses` with `__slots__` (`189eac3`)
- Abstract base classes for queue management, audio streaming, and Telegram command handling (`ec7044f`)
- Structured logging configuration with mandatory `group_id` enforcement (`1890092`)
- `RedisClient` with Sentinel support for high-availability connection management (`71acec5`)
- `RedisKeys` namespace builder with environment prefix and key families (queue, shadow, state, settings, cache, lock, track, active, rate limit) (`919b8dd`)
- Lua scripts for atomic Redis queue operations (enqueue, dequeue, acknowledge, nack, promote, clear) (`26f6511`)
- `msgpack` serialisation layer for domain models (`43f83d4`)
- `ConcreteQueueManager` (Redis-backed) with Lua-scripted atomic enqueue/dequeue/ack (`3d1b351`)

---

## [0.0.0] — 2026-03-14 — Repository Bootstrap

### Added

- Initial commit and file uploads (`caf0305`, `6327087`, `1f443d8`)
- Redis-backed queue manager prototype via PR #1 (`7322f7f`, `c06e669`)
- Repository scaffolding via PR #2 (`464e258`, `0dd6751`)

### Removed

- Legacy `README.md`, `.env.example`, `queue_manager.py`, and `docs/redis-schema.md` cleared for restructure (`bbca825`, `95ff964`, `7bdd0e9`, `1244d07`)

### Changed

- Placeholder print statement updated from `'Hello'` to `'Goodbye'` (`3f6555a`)
