# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests
clojure -X:test

# Run a single test namespace
clojure -X:test :nses '[genegraph.framework.app-test]'

# Start a dev REPL (includes Portal for data inspection)
clojure -M:dev

# Generate API docs
clojure -X:codox
```

## Architecture

This is a Clojure library (not an application) for building Kafka stream-processing apps. It is **data-driven and protocol-driven**: an entire application is described as a single map, initialized with `p/init`, then started with `p/start`.

### Core data flow

```
app-def map
  → p/init (dispatches on :type per component)
  → initialized app map (with component records wired together)
  → p/start (starts in order: system-processor → storage → topics → processors → http-servers)
```

### The event map

Events are plain Clojure maps with namespaced keys (`::event/` = `genegraph.framework.event/`). Interceptors receive and return event maps. Side effects (`event/store`, `event/delete`, `event/publish`) are accumulated on the map as data and executed **after** the interceptor chain completes — they are deferred, not immediate.

### Component wiring

Components reference each other by keyword. At `p/init` time, `app.clj` resolves these keyword references into actual component records. For example, a processor's `:subscribe :my-topic` becomes a direct reference to the initialized topic record.

### System topic

Every app has an internal `:system` `SimpleQueueTopic` auto-created and auto-subscribed by a system processor. Components publish lifecycle events (`:starting`, `:started`, `:up-to-date`, `:exception`) to it. You can override the system processor by defining one with `:subscribe :system` in `:processors`.

### Parallelism model

- `:processor` — single thread, sequential
- `:parallel-processor` — 3-thread pipeline (deserialize / process / effects). Use `:gate-fn` (a fn of `::event/data`) to serialize events with the same key while parallelizing across keys.

### Kafka offset handling

`:kafka-consumer-group-topic` tracks two offsets: the Kafka consumer group offset and an optional local `:backing-store` offset. On start, if the backing store lags behind, it replays events from Kafka with `:skip-publish-effects true` (rebuilding local state without re-publishing). The `:up-to-date` system event fires once the topic has caught up to the end offset at startup time.

### Transactional publishing

Set `:kafka-transactional-id` on a processor to wrap storage writes and Kafka publishes in a single Kafka transaction (exactly-once semantics). The processor's producer coordinates offset commits and publishes atomically.

### Key namespaces

| Namespace | Role |
|---|---|
| `genegraph.framework.app` | App init/wiring, startup/shutdown ordering |
| `genegraph.framework.protocol` | All protocols + `p/init` multimethod dispatch |
| `genegraph.framework.event` | Event keys, `event/store`, `event/publish`, `event/delete`, serialization |
| `genegraph.framework.processor` | `Processor` and `ParallelProcessor` records |
| `genegraph.framework.topic` | `SimpleQueueTopic` |
| `genegraph.framework.kafka` | Kafka topic records, producer/consumer logic, exception categories |
| `genegraph.framework.kafka.admin` | Topic provisioning utilities |
| `genegraph.framework.storage` | Storage protocols, `store-snapshot`/`restore-snapshot` |
| `genegraph.framework.storage.rocksdb` | RocksDB backend (Nippy serialization, LZ4 tar snapshots) |
| `genegraph.framework.storage.rdf` | Apache Jena TDB2 + SPARQL; use `rdf/tx` macro for reads |
| `genegraph.framework.storage.gcs` | Google Cloud Storage (streams as values) |
| `genegraph.framework.http_server` | Pedestal/http-kit HTTP server |
| `genegraph.framework.event.store` | Gzipped EDN event log utilities |

### RocksDB key encoding

Keys are encoded from Clojure types: `bytes` → raw; `String`/`Keyword` → 64-bit XX3 hash; `Long` → big-endian 8 bytes (supports range scans); `Sequential` → concatenation of each element hashed to 8 bytes.

### Examples

The `example/` directory contains runnable examples for Confluent Cloud, full app lifecycle, HTTP servers, parallel processors, Kafka transactions, and more. Consult these before adding new features.
