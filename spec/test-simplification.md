# Test Simplification Plan

## Overview

The test suite covers the framework thoroughly but has several recurring patterns that make tests
brittle in the face of legitimate refactoring. The problems fall into four categories:

1. **Internal implementation coupling** — tests assert on private data structures, exact record
   types, and exact function references instead of observable behavior.
2. **Over-fragmentation** — related assertions are split into many micro `testing` blocks, each
   expressing one edge at a time. A single behavioral test that sets up state once and checks all
   relevant outputs is easier to read and less sensitive to incidental change.
3. **Timing-based assertions** — `Thread/sleep` is used to wait for asynchronous work instead of
   synchronization primitives, making tests flaky on slow machines.
4. **Duplicated helpers** — `make-topic`, `make-store`, `make-processor`, and `deref-cp` are
   defined independently in multiple test namespaces.

---

## Category 1 — Internal Implementation Coupling

### 1a. `processor_test.clj` — `as-interceptors-test`

**Problem.** The test hard-codes the count (4) and the exact qualified names of framework
interceptors at exact vector positions:

```clojure
(is (= 4 (count (p/as-interceptors (make-processor)))))
(is (= :genegraph.framework.processor/app-state-interceptor (:name (nth ics 0))))
(is (= :genegraph.framework.processor/local-effects-interceptor (:name (nth ics 1))))
...
```

If a framework interceptor is renamed, split, or reordered, every assertion breaks even though
`p/process` behavior is unchanged.

**Fix.** Delete `as-interceptors-test` entirely. The behaviors it guards are already covered by:
- `process-app-state-injection-test` (state injection works)
- `process-store-effects-test` (local effects run)
- `process-publish-effects-test` (publish effects run)
- `process-completion-promise-test` (completion promise is delivered)

The only assertion worth keeping is that user interceptors are reachable — express this as an
observable outcome (e.g., a user interceptor is actually called) rather than as a positional name
check.

---

### 1b. `http_server_test.clj` — `endpoint->route-test`

**Problem.** The test checks exact positional elements of the Pedestal route vector:

```clojure
(is (= 5 (count route)))
(is (= "/api/resource" (nth route 0)))
(is (= :get (nth route 1)))
(is (= :route-name (nth route 3)))
(is (= :my-proc (nth route 4)))
```

This couples tests to Pedestal's internal route tuple format. If the framework adds or moves an
element, all assertions break.

**Fix.** Remove `endpoint->route-test` entirely. The integration tests `plain-route-handler-test`
and `processor-endpoint-handler-test` already verify that routes work end-to-end over real HTTP.
The internal shape of the intermediate route vector is an irrelevant implementation detail.

---

### 1c. `event_test.clj` — `store-effect-test` and `delete-effect-test`

**Problem.** These tests dig into the internal structure of the `::event/effects` list:

```clojure
(is (= 1 (count (::event/effects result))))
(is (= storage/write (:command effect)))
(is (= :my-store (:store effect)))
(let [[store-arg key-arg val-arg promise-arg] (:args effect)] ...)
(is (identical? (:commit-promise effect) promise-in-args))
```

The exact field names (`:command`, `:store`, `:args`, `:commit-promise`), their arrangement in
`:args`, and their identity relationships are internal contracts. Code that restructures the effect
representation while preserving execution behavior will break all of these.

**Fix.** Replace structure-inspection tests with execution-outcome tests. The effect machinery is
already exercised in `processor_test.clj` (`process-store-effects-test`, `process-publish-effects-test`)
and `app_test.clj` (`e2e-single-processor-test`). For `event_test.clj`, keep only:

- That `event/store` returns an updated event (does not mutate the original).
- That multiple `event/store` / `event/delete` calls accumulate (count increases).
- That `event/store` and `event/publish` operate on separate keys (`::event/effects` vs
  `::event/publish`) and don't interfere with each other.
- The round-trip through a real processor delivers the expected stored value (already in
  `processor_test`).

Remove all tests that inspect `:command`, `:args`, `:commit-promise` identity, or `:store` field
names.

Similarly, `record-offset-test` checks the `:command` function reference (`storage/store-offset`)
and the exact positional order of `:args`. Replace with a round-trip test that calls
`event/record-offset`, then actually executes the effect, and checks that the offset was stored.

---

### 1d. `topic_test.clj` — `init-test` Java field access

**Problem.**

```clojure
(is (= 10 (.remainingCapacity (:queue topic))))
```

This accesses the private `:queue` field of `SimpleQueueTopic` and calls a Java method on it to
verify buffer size, coupling the test to the internal implementation.

**Fix.** Verify buffer-size behavior through the public API instead:

```clojure
;; Fill the buffer, then check publish returns false.
(let [topic (p/init {:name :t :type :simple-queue-topic :buffer-size 10})]
  (dotimes [_ 10] (p/publish topic {}))
  (is (false? (p/publish topic {}))))
```

Or check via `p/status`:
```clojure
(is (= 10 (:remaining-capacity (p/status topic))))
```

---

### 1e. Record type assertions vs. protocol satisfaction

**Problem.** Throughout the test suite, tests use `instance?` with fully-qualified record names:

```clojure
(instance? genegraph.framework.app.App ...)
(instance? genegraph.framework.topic.SimpleQueueTopic ...)
(instance? genegraph.framework.processor.Processor ...)
(instance? genegraph.framework.storage.atom.AtomStore ...)
(instance? genegraph.framework.storage.rocksdb.RocksDBInstance ...)
(instance? genegraph.framework.http_server.Server ...)
```

Renaming or splitting a record invalidates these tests even when the public protocols are unchanged.

**Fix.** Replace `instance?` checks with `satisfies?` checks against the protocols the component
is expected to implement. For example:

```clojure
;; Instead of:
(is (instance? genegraph.framework.topic.SimpleQueueTopic topic))
;; Write:
(is (satisfies? p/Consumer topic))
(is (satisfies? p/Publisher topic))
```

Keep one `instance?` check per component type only in the dedicated `init-test` for that component,
where establishing the concrete type is the explicit goal. Remove all others.

---

## Category 2 — Over-fragmentation

### 2a. `app_test.clj` — init / system-topic / system-processor tests

`init-returns-app-record-test`, `init-system-topic-test`, and `init-default-system-processor-test`
collectively make 8 separate assertions with 3 separate `p/init` calls just to verify that a
minimal app initializes correctly. Merge into a single `init-minimal-app-test`:

```clojure
(deftest init-minimal-app-test
  (let [app (p/init (minimal-app-def))]
    (is (satisfies? p/Lifecycle app))
    (is (map? (:topics app)))
    (is (contains? (:topics app) :system))
    (is (satisfies? p/Consumer (get-in app [:topics :system])))
    (is (= 1 (count (:topics app))))
    (is (contains? (:processors app) :default-system-processor))))
```

### 2b. `topic_test.clj` — `status-test`

Seven separate `testing` blocks create independent topics and check individual `status` fields.
These can be consolidated into two tests: one for an empty queue, one that exercises writes and
polls:

```clojure
(deftest status-invariants-test
  (let [topic (make-topic :buffer-size 4)]
    ;; Empty
    (let [s (p/status topic)]
      (is (= 0 (:queue-size s)))
      (is (= 4 (:remaining-capacity s))))
    ;; After two publishes
    (p/publish topic {})
    (p/publish topic {})
    (let [s (p/status topic)]
      (is (= 2 (:queue-size s)))
      (is (= 2 (:remaining-capacity s)))
      (is (= 4 (+ (:queue-size s) (:remaining-capacity s)))))
    ;; After one poll
    (p/poll topic)
    (let [s (p/status topic)]
      (is (= 1 (:queue-size s)))
      (is (= 3 (:remaining-capacity s))))))
```

### 2c. `processor_test.clj` — `processor-status-test`

Five separate `testing` blocks each call `make-processor` independently. Merge into one test that
checks stopped → running → stopped transitions on a single instance.

### 2d. `rocksdb_test.clj` — `rocksdb-instance-init-test`

Six separate `testing` blocks each call `p/init` with a fresh temp path. Merge protocol
satisfaction checks into one let-block:

```clojure
(deftest rocksdb-instance-init-test
  (let [path (temp-path)
        r    (p/init {:name :test-db :type :rocksdb :path path})]
    (is (satisfies? storage/HasInstance r))
    (is (satisfies? p/Lifecycle r))
    (is (satisfies? p/Resetable r))
    (is (satisfies? p/Status r))
    (is (nil? (storage/instance r)))
    (let [s (p/status r)]
      (is (= :test-db (:name s)))
      (is (= path (:path s)))
      (is (= :stopped (get-in s [:status :status]))))))
```

---

## Category 3 — Timing-Based Assertions

### 3a. `processor_test.clj` — stop-then-check-no-processing

```clojure
(p/stop proc)
(Thread/sleep 300) ; allow the loop thread to notice the stop
(p/publish in-topic ...)
(is (= :timeout (deref cp 400 :timeout)))
```

The sleep is a fixed wait for the polling loop to exit. This is inherently flaky. Instead, poll
`p/status` until `:stopped`:

```clojure
(p/stop proc)
(let [deadline (+ (System/currentTimeMillis) 2000)]
  (while (and (= :running (:status (p/status proc)))
              (< (System/currentTimeMillis) deadline))
    (Thread/sleep 25)))
(p/publish in-topic ...)
(is (= :timeout (deref cp 400 :timeout)))
```

Alternatively, introduce a shared `wait-for` helper (already present in `kafka_test.clj` — move it
to a shared test util namespace).

### 3b. `processor_test.clj` — `processor-exception-resilience-test`

```clojure
(Thread/sleep 300) ; Give the loop time to process (and catch the exception)
```

Replace with a synchronization mechanism: give the first event a `::event/completion-promise` and
wait for it to be realized (even on failure the processor should deliver it, or catch the exception
and move on). If the processor does not deliver on exception, use `wait-for` on `@first-event?`
becoming false.

### 3c. `topic_test.clj` — `timer-status-test` "status is :stopped after stop"

```clojure
(p/stop topic)
(Thread/sleep 100) ; allow virtual thread to exit
(is (= :stopped ...))
```

Use `wait-for` instead of a fixed sleep.

### 3d. `topic_test.clj` — `timer-stop-test`

```clojure
(Thread/sleep 150) ; wait for virtual thread to exit
```

Same fix: use `wait-for`.

---

## Category 4 — Duplicated Helpers

### 4a. Shared test utilities namespace

`make-topic`, `make-store`, `make-processor`, and `deref-cp` are defined in both
`processor_test.clj` and `app_test.clj`. The `wait-for` helper from `kafka_test.clj` would be
useful in `processor_test.clj` and `topic_test.clj`.

**Fix.** Create `test/genegraph/framework/test_util.clj` with:
- `make-topic` (simple-queue-topic, 100ms timeout)
- `make-store` (atom-store)
- `make-processor` (processor with empty interceptors/topics/storage)
- `deref-cp` (2-second timeout deref)
- `wait-for` (25ms-polling predicate check with deadline)

All test namespaces require this and remove their local copies.

---

## Category 5 — Low-Value or Redundant Tests

### 5a. `kafka_test.clj` — `consumer-record->event-test` repeated combinations

The test includes two nearly-identical cases: one for offset 0 / partition 0, one for offset 999 /
partition 2. The second adds little coverage. Collapse into one parameterized case.

### 5b. `topic_test.clj` — `completion-promise-injection-test`

Four tests verify different facets of promise injection. Tests 3 and 4 are redundant: "keeps
existing promise" and "is not replaced by a new one" test the same invariant twice with slightly
different phrasing. Remove the fourth.

### 5c. `kafka_test.clj` — CG-topic and reader-topic supervisor tests

`cg-topic-clean-stop-test` / `reader-topic-clean-stop-test` and
`cg-topic-restart-on-recoverable-exception-test` / `reader-topic-restart-on-recoverable-exception-test`
are structurally identical. Extract a parameterized helper:

```clojure
(defn- supervisor-loop-scenario
  [make-topic-fn deliver-offsets-fn poll-fn-key create-fn-key extra-redefs]
  ...)
```

Then each test becomes a one-liner call with the appropriate arguments.

---

## Summary of Changes by File

| File | Action |
|---|---|
| `processor_test.clj` | Delete `as-interceptors-test`; merge `processor-status-test`; replace `Thread/sleep` with `wait-for`; require `test_util` |
| `app_test.clj` | Merge init/system-topic/system-processor tests; replace `instance?` with `satisfies?`; require `test_util` |
| `topic_test.clj` | Remove Java `.remainingCapacity` access; consolidate `status-test`; remove redundant completion-promise test; replace `Thread/sleep` with `wait-for` |
| `http_server_test.clj` | Delete `endpoint->route-test` |
| `event_test.clj` | Remove `:command`/`:args`/`:commit-promise`-identity assertions from `store-effect-test`, `delete-effect-test`, `record-offset-test`; keep count and non-interference tests |
| `kafka_test.clj` | Move `wait-for` to `test_util`; consolidate CG/reader supervisor tests; collapse `consumer-record->event-test` |
| `storage/atom_test.clj` | Merge init protocol-satisfaction checks |
| `storage/rocksdb_test.clj` | Merge `rocksdb-instance-init-test` protocol-satisfaction checks |
| `test/genegraph/framework/test_util.clj` | **Create** — shared helpers |

## Guiding Principle

A test should break only when observable, protocol-level behavior changes. It should not break
because a private data structure was reorganized, a record was renamed, or a framework interceptor
was given a new name. Prefer checking "does the value get stored?" over "is `:command` equal to
`storage/write`?".
