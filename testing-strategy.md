# Testing Strategy: Genegraph Framework

## Current State

The existing test suite (`test/genegraph/framework/app_test.clj`) contains a single placeholder test (`(is true)`). The framework itself is non-trivial: it coordinates Kafka consumers, producers, local storage engines, interceptor chains, threading, and async promises. This document proposes a layered testing strategy that builds from pure unit tests up to full integration tests, keeping tests fast and infrastructure-free where possible.

---

## Guiding Principles

1. **Test the contract, not the implementation.** Each component exposes protocols (`Lifecycle`, `Consumer`, `Publisher`, `Status`, etc.). Tests should verify protocol behavior, not internal state.
2. **Push infrastructure to the boundary.** The `SimpleQueueTopic` and `AtomStore` are drop-in replacements for Kafka and RocksDB within a single process. Most behavioral tests should use these. Kafka and RocksDB tests are reserved for integration suites that can be run selectively.
3. **Make async deterministic.** Completion promises and delivery guarantees are core to the framework. Tests must await promises with explicit timeouts rather than using `Thread/sleep`.
4. **Isolate side effects.** Storage writes, publishes, and offset commits are deferred on the event map. Tests can verify the *accumulated effects on the event map* without executing them.
5. **One namespace per component.** Mirror the `src/` tree in `test/`, so each source namespace has a corresponding test namespace.

---

## Test Levels

### Level 1: Pure Unit Tests

No I/O, no threads, no external services. These test functions that take data and return data.

### Level 2: Component Tests

Tests for individual records (`SimpleQueueTopic`, `Processor`, `AtomStore`, `RocksDBInstance`). May start threads or open files but do not require Kafka or cloud services.

### Level 3: Integration Tests

Full app lifecycle tests using in-process topics and storage. Require no external services. These are the primary behavioral tests.

### Level 4: Infrastructure Tests

Tests against real Kafka (local or containerized) and real cloud storage. Run separately from the default `clojure -X:test` command.

---

## Test Namespaces and Coverage Plan

### `genegraph.framework.event-test` (Level 1)

The event namespace is purely functional with no I/O — highest priority for unit tests.

**Serialization/deserialization:**
- `:json` format: `deserialize` parses JSON string in `::value` into `::data` map with keyword keys; `serialize` writes `::data` back to `::value`
- `:edn` format: round-trip identity for any Clojure data structure
- `:default` (no format): `deserialize` is a no-op; `serialize` passes `::data` through as `::value`
- Edge cases: malformed JSON/EDN should be caught; nil `::value` behavior

**Effect accumulation:**
- `event/store` adds an entry to `::effects` with `:command storage/write`, `:args`, and `:commit-promise`; the event is otherwise unchanged
- `event/delete` adds an entry to `::effects` with `:command storage/delete`
- `event/publish` adds an entry to `::publish` with a `:commit-promise`
- Calling `store`/`delete`/`publish` multiple times accumulates multiple entries (they should not overwrite each other)
- `event/store` references the correct storage instance from `::storage/storage` by the store key

**Offset recording:**
- `record-offset` appends to `::effects` with `:command storage/store-offset` and the correct args

### `genegraph.framework.topic-test` (Level 2)

Tests for `SimpleQueueTopic`.

**Basic publish/poll contract:**
- `p/publish` followed by `p/poll` returns the published event (with `::event/topic` set to the topic's `:name`)
- `p/poll` on an empty queue returns `nil` after the configured timeout
- `p/publish` delivers the event's `:commit-promise` if present

**Capacity and backpressure:**
- A topic initialized with `:buffer-size 1` accepts a second `publish` only after the first is polled
- `p/status` returns correct `:queue-size` and `:remaining-capacity`

**Completion promise injection:**
- Events without `::event/completion-promise` receive one after `p/poll`

### `genegraph.framework.processor-test` (Level 2/3)

Tests for `Processor` and `ParallelProcessor` using `SimpleQueueTopic` and `AtomStore`.

**Interceptor chain execution:**
- A processor with a single interceptor applies it to each polled event
- The interceptor chain has access to `::storage/storage` refs and `::event/topics`
- Exceptions thrown in an interceptor are caught and logged; the processor keeps running

**Effect execution:**
- After the chain, `perform-publish-effects!` calls `p/publish` for each entry in `::event/publish`
- `update-local-storage!` calls the `:command` function for each entry in `::effects`
- `skip-publish-effects true` suppresses Kafka publishes (used during replay)
- `skip-local-effects true` suppresses local storage writes

**Completion promises:**
- `::event/completion-promise` is delivered `true` after effects complete without error
- `::event/completion-promise` is delivered `false` if an effect's `:commit-promise` is not delivered `true`

**Processor lifecycle:**
- `p/start` sets `:status :running`; `p/stop` sets `:status :stopped`
- After stop, the polling loop exits

**`p/as-interceptors` (for HTTP):**
- Returns a vector starting with `app-state-interceptor`, `local-effects-interceptor`, `publish-effects-interceptor`, `deliver-completion-promise-interceptor`, then user interceptors

**Parallel processor:**
- Events processed concurrently arrive in `::event/completion-promise` order regardless of processing duration
- `:gate-fn` serializes events with the same key: a second event with key K does not begin processing until the first event with key K delivers its completion promise
- The deserialized-event-queue and effect-queue status appear in `p/status`

### `genegraph.framework.app-test` (Level 3)

Full app lifecycle using only in-process components. This is the most important integration test suite.

**App initialization (`p/init`):**
- Given a valid app-def with `:simple-queue-topic` topics and `:atom-store` storage, `p/init` returns an `App` record
- All topic keyword references in processor `:subscribe` fields are resolved to initialized topic records
- All storage keyword references are resolved
- The system topic `:system` is auto-created and added to `:topics`
- The default system processor is added when no `:subscribe :system` processor is defined
- A custom system processor (`:subscribe :system`) is used when provided

**App startup/shutdown:**
- `p/start` starts components in order: system-processor → storage → topics → processors → http-servers
- `p/stop` reverses the order: http-servers → processors → storage → topics → system-processor
- After `p/start`, all components report `:running` status; after `p/stop`, all report `:stopped`

**End-to-end event flow:**
- A minimal app with one `SimpleQueueTopic` and one `Processor` with a store interceptor: publish an event to the topic, await the completion promise, verify the stored value appears in the `AtomStore`
- A pipeline: processor A reads from topic-1 and publishes to topic-2; processor B reads from topic-2 and stores to atom-store. Verify the final value arrives.

**System topic:**
- Components publish `:starting` and `:started` events to the system topic
- A custom system processor interceptor can observe these events
- The `register-listener` / `trigger-listeners` mechanism: register a listener with a predicate, publish a matching system event, and verify the promise is delivered

**`p/reset`:**
- Calls `p/reset` on all topics and storage that satisfy `Resetable`

**`p/status`:**
- Returns a map with `:topics`, `:storage`, `:processors`, `:http-servers` keys, each containing per-component status

**Component wiring edge cases:**
- App-def with no `:storage` key initializes successfully
- App-def with no `:http-servers` key initializes successfully
- Processor that references a nonexistent topic key should produce a clear error

### `genegraph.framework.storage.rocksdb-test` (Level 2)

Uses real RocksDB, but only on the local filesystem. Tests run against a temp directory deleted after each test.

**Key encoding (`k->bytes`):**
- `String` → 8-byte XX3 hash (same string produces same bytes; different strings produce different bytes with overwhelming probability)
- `Long` → big-endian 8 bytes; small longs produce a smaller byte value than large longs (enables range scan ordering)
- `Keyword` → same as `(str keyword)` string encoding
- `byte[]` → identity (raw passthrough)
- Sequential (vector/list) → concatenation of each element's 8-byte encoding; `[:a :b]` differs from `[:b :a]`

**Read/write/delete:**
- Write then read round-trips any Nippy-serializable Clojure value
- Read a key that was never written returns `::storage/miss`
- Delete a written key; subsequent read returns `::storage/miss`
- Write with `commit-promise` delivers `true`; delete with `commit-promise` delivers `true`

**Range scan:**
- Write several `Long` keys; `scan db 1 4` returns values for keys 1, 2, 3 (exclusive upper bound)
- Write several sequential-keyed values; `scan db :prefix` returns all values sharing that prefix
- `range-delete` removes all keys matching prefix; subsequent scan returns empty

**Topic offset tracking:**
- `store-offset` + `retrieve-offset` round-trips; missing offset returns `::storage/miss`

**Snapshot (`RocksDBInstance` lifecycle):**
- `store-snapshot` creates a checkpoint, archives it, and cleans up the checkpoint directory
- After `reset` with `:destroy-snapshot true`, the local DB directory is destroyed

**`prefix-range-end`:**
- For a 16-byte key (two 8-byte hashes), the range-end has the same prefix byte and the last 8 bytes incremented by 1

### `genegraph.framework.storage.atom-test` (Level 1)

**Read/write/delete:**
- Standard `IndexedRead`/`IndexedWrite`/`IndexedDelete` contract
- Write with `commit-promise` delivers `true`
- `p/status` reports `:count` matching the number of stored keys

### `genegraph.framework.event.store-test` (Level 2)

Uses temp files.

**Round-trip:**
- `with-event-writer` + `prn` then `with-event-reader` + `event-seq` returns the same sequence of maps
- `store-events` then reading via `with-event-reader` produces the written sequence
- The file is gzip-compressed: reading the raw bytes is not valid UTF-8/EDN

**Edge cases:**
- Empty file → `event-seq` returns empty sequence
- File with one event → sequence of one

### `genegraph.framework.kafka-test` (Level 1 for pure functions, Level 4 for Kafka)

**Pure function tests (no Kafka required):**
- `consumer-record->event`: given a mock `ConsumerRecord` object, returns a map with the correct `::event/key`, `::event/value`, `::event/offset`, etc.
- `event->producer-record`: given an event map, returns a `ProducerRecord` with the correct topic, key, value, partition, timestamp
- `in-exception-category?`: returns true when exception is an instance of a class in the category list
- `exception-outcome`: maps `ApplicationRecoverableException` → `:restart-client`, `RetriableException` → `:retry-action`, `InvalidConfigurationException` → `:halt-application`
- `topic-up-to-date?`: given a state atom with specific offset values, returns the correct boolean
- `backing-store-lags-consumer-group?`: given delivered/undelivered promises, returns correct result or `:timeout`

**Infrastructure tests (require local Kafka):**
- Full `KafkaConsumerGroupTopic` lifecycle: start, publish, poll, stop
- Offset tracking: consumer group offset vs. local backing store offset
- Local event replay when backing store lags consumer group
- `KafkaReaderTopic`: always reads from offset 0, sets `skip-publish-effects true`
- Transactional producer: exactly-once delivery with offset commit

### `genegraph.framework.http-server-test` (Level 3)

Uses an actual HTTP server on an ephemeral port.

**Route setup:**
- A processor record can serve as a Pedestal interceptor chain via `p/as-interceptors`
- `endpoint->route` produces a valid Pedestal route tuple

**Request handling:**
- An endpoint backed by a processor interceptor chain receives the request context, runs the chain, and returns a response
- The server starts on the configured port and stops cleanly

---

## Test Infrastructure and Helpers

### Test fixtures

```clojure
;; Example: create and destroy a temp RocksDB for each test
(defn with-temp-rocksdb [test-fn]
  (let [path (str (System/getProperty "java.io.tmpdir")
                  "/rocksdb-test-" (random-uuid))]
    (try
      (test-fn path)
      (finally
        (rocksdb/destroy path)))))
```

### Async test helpers

Since processors are threaded and completion promises are the primary signal, tests need a utility to await delivery with a timeout:

```clojure
(defn await-promise
  ([p] (await-promise p 5000))
  ([p timeout-ms]
   (let [result (deref p timeout-ms ::timeout)]
     (when (= ::timeout result)
       (throw (ex-info "Promise not delivered within timeout" {:timeout-ms timeout-ms})))
     result)))
```

### Minimal in-process app builder

A test helper that constructs a minimal app with in-process components, avoiding Kafka and filesystem storage:

```clojure
(defn minimal-app [& {:keys [interceptors on-system-event]}]
  {:type :genegraph-app
   :topics {:in {:name :in :type :simple-queue-topic}
            :out {:name :out :type :simple-queue-topic}}
   :storage {:store {:name :store :type :atom-store}}
   :processors {:main {:name :main
                       :type :processor
                       :subscribe :in
                       :interceptors interceptors}}})
```

### System event listener helper

Wraps the register-listener mechanism to block a test until a specific system event arrives:

```clojure
(defn await-system-event [app predicate timeout-ms]
  (let [p (promise)]
    ;; publish a :register-listener event to the system topic
    ;; ...
    (deref p timeout-ms ::timeout)))
```

---

## Test Organization and Running

### Directory structure

```
test/
  genegraph/
    framework/
      app_test.clj
      event_test.clj
      topic_test.clj
      processor_test.clj
      kafka_test.clj          ;; pure functions only by default
      http_server_test.clj
      event/
        store_test.clj
      storage/
        atom_test.clj
        rocksdb_test.clj
        rdf_test.clj
```

### Test aliases

Add to `deps.edn`:

```clojure
:unit   {:exec-args {:nses [genegraph.framework.event-test
                            genegraph.framework.topic-test
                            genegraph.framework.storage.atom-test
                            genegraph.framework.event.store-test
                            genegraph.framework.kafka-test]}}  ;; pure fns only

:integration {:exec-args {:nses [genegraph.framework.app-test
                                 genegraph.framework.processor-test
                                 genegraph.framework.storage.rocksdb-test
                                 genegraph.framework.http-server-test]}}

:kafka {:exec-args {:nses [genegraph.framework.kafka-integration-test]}}
```

The default `:test` alias runs both unit and integration suites (everything except `:kafka`).

---

## Prioritized Implementation Order

The following order delivers the most value earliest:

1. **`event-test`** — Covers pure functions that underpin the entire framework. Fast, no setup. Start here.
2. **`topic-test`** — `SimpleQueueTopic` is used by every in-process test. Must be solid before testing processors.
3. **`storage/atom-test`** — Needed by processor and app tests as a storage substitute.
4. **`processor-test`** — Core behavioral tests for the interceptor chain and effect execution. Use `SimpleQueueTopic` + `AtomStore`.
5. **`app-test`** — Full lifecycle and wiring tests. Depends on all of the above.
6. **`storage/rocksdb-test`** — Important but requires filesystem setup. Key encoding is subtle and must be verified.
7. **`event/store-test`** — Covers the gzipped event log used for replay.
8. **`kafka-test` (pure functions)** — Exception categorization, record/event conversion, offset logic.
9. **`http-server-test`** — Validates HTTP integration after the rest is solid.
10. **`kafka-test` (infrastructure)** — Requires a running Kafka; run separately in CI with a Kafka container.

---

## Known Risks and Gaps to Address in Tests

- **`rocksdb.clj:22`** — `vec->key-bytes` calls `(println "vector")` and is not used anywhere. Likely dead code; confirm and remove.
- **`rocksdb.clj:127`** — `swap! state :status :started` (line 127) is missing `assoc`; this is a bug (passes a keyword as the function, not `assoc`). Write a test that starts a `RocksDBInstance` and verify it doesn't throw.
- **`kafka.clj:799`** — `restart-kafka-producer!` calls `(swap! state :status :restarting)` — same missing-`assoc` bug pattern. Document in test as a known issue.
- **`processor.clj:279`** — `assoc-in` called with a non-vector third argument `(:gates k new-gate)`; this is a bug in `get-or-create-gate`. Write a gate test to expose it.
- **`kafka.clj:851`** — `GenegraphKafkaProducer/stop` calls `(swap! state :status :stopped)` missing `assoc`. Test the full producer lifecycle.
- **Timeout behavior** — `effect-error` uses a 1-hour default timeout (`(* 1000 60 60)`). Tests must supply `::event/effect-timeout` explicitly to avoid hanging.
- **Concurrency** — `ParallelProcessor` gate state uses `assoc-in` with a non-sequential path (same bug as above). Gate tests should verify ordering guarantees under concurrent load.
