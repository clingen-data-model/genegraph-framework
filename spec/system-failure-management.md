# System Failure Management Strategy

This document describes how to handle the three major classes of runtime failure in a genegraph-framework application:

1. Kafka unavailability
2. Internal programming errors during message processing
3. External API endpoint failures during processing

The framework already provides several primitives that failure management must build on: the **system topic** (a `SimpleQueueTopic` every app gets automatically), the **`p/exception` protocol function** (sets component state to `:exception` and publishes to the system topic), the **`p/system-update` protocol function** (lifecycle event notifications), the **Kafka exception taxonomy** in `kafka/exception-outcome` (`ApplicationRecoverableException` → `:restart-client`, `RetriableException` → `:retry-action`, `InvalidConfigurationException` → `:halt-application`), and the **`::event/completion-promise`** mechanism by which topics track whether an event was fully processed.

---

## 1. Kafka Unavailability

### Failure modes

- Broker unreachable at startup (consumer/producer `start` fails)
- Broker becomes unreachable after startup (polling or publishing throws)
- Transient network partition (milliseconds to seconds)
- Extended outage (minutes to hours)
- Consumer group rebalance / coordinator loss

### Current behavior

`p/exception` is called from the consumer thread catch blocks in `KafkaConsumerGroupTopic`, `KafkaReaderTopic`, and `produce-local-events`. This sets `:status :exception` on the component atom and fires a system-topic event. The consumer thread then exits permanently. The producer's `report-producer-error` only logs — it does not halt or restart the producer.

### Strategy

**Classify before acting.** The existing `kafka/exception-outcome` function already maps Java exception types to outcome keywords. This classification must actually drive behavior, not just be computed and discarded.

| Outcome | Kafka exception type | Recommended action |
|---|---|---|
| `:retry-action` | `RetriableException` | Retry the poll/send with exponential backoff (cap at ~30 s). Stay in the consumer loop. |
| `:restart-client` | `ApplicationRecoverableException` | Close and recreate the `KafkaConsumer`/`KafkaProducer`, then resume. Update `:restarts` counter in state atom. |
| `:halt-application` | `InvalidConfigurationException` | Call `p/exception`, publish to system topic with `:severity :fatal`, then stop the entire app cleanly. |
| `:unknown-outcome` | anything else | Treat as `:restart-client` up to a restart limit (e.g. 5 within 60 s), then escalate to `:halt-application`. |

**Backoff.** Retries and restarts must use exponential backoff with jitter to avoid thundering-herd reconnects. A simple model: wait `(min (* base-ms (Math/pow 2 attempt)) max-ms)` milliseconds, then add up to 20% random jitter. Reasonable defaults: `base-ms` = 500, `max-ms` = 30000.

**Restart budget.** Track restart attempts in the component's state atom (`:restarts`, `:last-restart-at`). If the restart rate exceeds a threshold (e.g. 5 restarts within the last 60 seconds) without a successful poll completing, treat it as a fatal failure. This prevents a permanently broken broker connection from spinning in an infinite restart loop.

**Local state preservation during outage.** Because the framework supports a backing-store offset (`:kafka-consumer-group-topic` tracks both the consumer-group offset and the local backing-store offset), the application can continue to serve reads from local RocksDB or RDF state during a Kafka outage. The HTTP server and its processors that read from local storage need not stop. Only the Kafka-consuming processor(s) are affected. This is the primary mechanism for graceful degradation under Kafka unavailability.

**Up-to-date flag.** When a Kafka consumer restarts after an outage, it will replay events from the last committed offset. The existing `topic-up-to-date?` / `deliver-up-to-date-event-if-needed` logic handles this correctly: the `:up-to-date` system event fires only after catching up, preventing downstream processors from serving stale data as if it were current.

**Queue backpressure.** When Kafka is slow but not down, the internal `ArrayBlockingQueue` (default size 100) will fill. The comment in `kafka.clj` at line 81 already flags this as a loose end. Strategy: when `deliver-event` cannot place an event within a bounded timeout (e.g. 10 s), publish a `:queue-full` system event with `:source` and `:queue-size`, then block briefly and retry. Do not drop events silently.

**What to put in the system topic.** Every Kafka exception event published to the system topic should include: `:type :kafka-exception`, `:outcome` (the keyword from `exception-outcome`), `:component` (topic/processor name), `:exception-class`, `:message`, `:restart-count`, and `:timestamp`. Custom system processors can forward these to an alerting channel (PagerDuty, Slack, etc.) or expose them via the HTTP status endpoint.

---

## 2. Internal Programming Errors During Message Processing

### Failure modes

- An interceptor throws an uncaught exception (bad input data, NPE, deserialization failure, assertion violation)
- An effect fails (storage write throws, Kafka publish times out)
- A poison pill: a single malformed message that will always throw, causing the processor to loop on the same offset forever if retried naively
- A slow message that causes effect timeouts (the 1-hour default in `::event/effect-timeout`)

### Current behavior

In `Processor.start`, the per-event `try/catch` at line 243 logs the exception and **continues the processing loop** — the processor does not halt. The commented-out reset lines (`#_(reset! state :error)`, `#_(p/system-update this {:state :error})`) are dead code. In `ParallelProcessor`, the three pipeline threads each have their own `try/catch` that similarly logs and continues. Effect errors are tracked via `:commit-promise` in `update-local-storage!` and surface through `effect-error` → `deliver-completion-promise`, but the result is only used to fire the completion promise — nothing acts on a `false` completion at the topic level.

### Strategy

**Distinguish error types at the event level.** Not all exceptions warrant the same response:

- **Deserialization failure**: the raw bytes cannot be read as the expected format. These are almost certainly permanent (poison pill). The event should be routed to a dead-letter store immediately without retry.
- **Interceptor logic exception**: an unhandled exception in user-defined interceptor code. May be transient (external call failed) or permanent (programming error). Should be retried a small number of times (e.g. 3) with backoff before being treated as a dead letter.
- **Effect failure**: storage or publish failed after the interceptor chain completed. Storage failures in particular may leave the system in an inconsistent state. These require special handling (see below).

**Add an error interceptor at the front of every chain.** Every interceptor chain should include a top-level error interceptor as its first entry. This interceptor's `:error` handler (Pedestal interceptor chains support `:error` as the third callback) catches any exception thrown further down the chain, attaches structured error data to the event map under e.g. `::event/error`, and converts the event into an error event. This prevents the raw exception from propagating out of `interceptor-chain/execute` and gives downstream interceptors (including effects) a chance to route it.

```clojure
;; Sketch — not production-ready
(def error-capture-interceptor
  {:name ::error-capture
   :error (fn [ctx ex-info]
            (-> ctx
                (assoc ::event/error {:exception (ex-cause ex-info)
                                      :interceptor (-> ex-info ex-data :interceptor)
                                      :stage      (-> ex-info ex-data :stage)})
                (dissoc :io.pedestal.interceptor.chain/error)))})
```

**Dead-letter storage.** Every app should configure a dead-letter store — at minimum a RocksDB table keyed by `[topic offset]` storing the raw event plus the error map. When an event is identified as a dead letter (retries exhausted, or deserialization failure), it is written there instead of being discarded. A companion HTTP endpoint can expose dead-letter entries for inspection and manual replay.

**Retry counter on the event.** Attach a retry count to the event map (e.g. `::event/retry-count`). When a retryable error occurs, re-enqueue the event on the source topic's internal queue with an incremented count. Cap retries at a configurable limit (default 3). After the cap, route to dead-letter storage and publish a `:dead-letter` system event.

**Effect failure and state consistency.** When `update-local-storage!` catches an exception from a storage command, it currently delivers the exception to the `:commit-promise`. The processor observes this as a `false` completion but takes no action. The risk is a split state: the interceptor chain ran and may have partially committed some effects while others failed. Strategy:

- For RocksDB: operations should be grouped into a single `WriteBatch` where possible, making them atomic. The framework's `event/store` and `event/delete` effects should be accumulated into a batch and applied together.
- For RDF (TDB2): use the existing `rdf/tx` macro boundary — all writes in a transaction either commit or roll back.
- When an effect fails after a successful chain run, publish a `:effect-failure` system event with full context. Do not silently continue to the next event without resolving whether state is consistent.

**Effect timeouts.** The default `::event/effect-timeout` is one hour (line 110 in `kafka.clj`). This is far too long to be useful as a monitoring signal. A reasonable default is 60 seconds. When a timeout fires, the stack trace of the processing thread is already captured (lines 113–118 in `kafka.clj`). The timeout path should also publish a `:effect-timeout` system event containing the stack trace and event metadata.

**ParallelProcessor additional risk.** In `ParallelProcessor`, the deserialization, processing, and effect stages run in three separate threads. An exception in the processing thread results in a future that holds an exception. When the effect thread calls `@event-future`, it will throw. The current `try/catch` in the effect thread catches this but logs only `:record ::ParallelProcessor` — no event context. This needs to capture the event and route it to dead-letter storage.

**Poison pill detection.** A message that always throws will be re-enqueued indefinitely if retried. Detect poison pills by tracking per-offset failure counts in the component state atom. If the same offset fails more than the retry limit, declare it a poison pill, write it to dead-letter storage, advance past it (commit the offset), and emit a `:poison-pill` system event. Never allow a single bad message to permanently stall a processor.

---

## 3. External API Endpoint Failures During Processing

### Failure modes

- HTTP 5xx from an external service (transient server error)
- Connection timeout / DNS failure (network issue)
- HTTP 429 Too Many Requests (rate limiting)
- HTTP 4xx (bad request — likely a permanent error for this specific payload)
- Partial success: the API call succeeded but returned unexpected data

### Current behavior

The framework has no built-in primitives for external API calls. These happen entirely inside user-defined interceptors. There is no structured retry, circuit breaker, or timeout management at the framework level.

### Strategy

**Treat external API failures as interceptor errors.** The error-capture interceptor described in Section 2 will catch exceptions thrown by HTTP client calls. Application code should translate HTTP error responses into exceptions (or error maps on the event) so that the retry and dead-letter logic applies uniformly.

**Classify by recoverability inside the interceptor:**

| HTTP status | Classification | Action |
|---|---|---|
| 5xx | Transient | Retry with backoff |
| 429 | Transient (rate-limited) | Retry after `Retry-After` header delay, or backoff |
| 408, 503 | Transient | Retry with backoff |
| 4xx (except 429) | Permanent | Dead-letter immediately; retry will not help |
| Network timeout | Transient | Retry with backoff |
| Connection refused | Transient (short-term) or permanent (config error) | Retry limited times, then circuit-break |

**Circuit breaker per external endpoint.** A circuit breaker prevents the processor from hammering a failing external service. Maintain a small state atom per external dependency with three states:

- `:closed` — normal operation, calls pass through
- `:open` — calls are blocked immediately (fail fast) and a `:circuit-open` system event is published; the circuit opens after N consecutive failures within a time window
- `:half-open` — after a cooldown period, one probe call is allowed; on success, circuit closes; on failure, it reopens

Circuit breaker state should be stored in the component state atom (or a dedicated atom passed via `::event/metadata`) so it persists across events but resets on processor restart. Expose circuit state via the `/status` endpoint.

**Timeouts on every external call.** Every HTTP call must have an explicit timeout. A call without a timeout can block a processor thread indefinitely. Recommended: connect timeout 5 s, read timeout 30 s (adjust to the API's SLA). On timeout, throw an exception that the error-capture interceptor catches as a transient error.

**Retry with backoff inside the interceptor.** For transient errors, retry within the same interceptor call (not by re-enqueuing) when the retry count is low (1–2 retries with a short delay). This avoids re-deserializing the event and keeps the latency impact small. After the in-interceptor retry budget is exhausted, throw so the outer retry/dead-letter logic takes over.

**Bulkhead: don't let one slow API starve the processor.** If a processor calls multiple external APIs, ensure that a slowdown in one does not block the thread pool for others. In `ParallelProcessor`, gate functions (`:gate-fn`) already provide per-key ordering; ensure that gate keys are not so coarse that a single stuck API call serializes all processing. In `Processor` (single-threaded), use per-call timeouts as the bulkhead.

**Emit `:api-failure` system events.** Every external API error that is not immediately retried should publish a `:api-failure` system event including: `:endpoint` (the URL or service name, not raw credentials), `:status-code` (if HTTP), `:retry-count`, `:event-key`, and `:circuit-state`. This gives the system processor and any monitoring hooks a consistent stream of external health signals.

---

## Cross-Cutting: The System Topic as the Error Bus

Every failure category above should publish structured events to the system topic. The system topic is the single place where all failure signals converge. Applications should provide a custom system processor (by defining a processor with `:subscribe :system`) that:

1. **Logs** every event at appropriate severity (`:info` for lifecycle, `:warn` for transient errors, `:error` for retries exhausted, `:fatal` for halt conditions).
2. **Exposes health via HTTP.** A `/healthz` endpoint backed by a processor that queries the app's `p/status` map can report per-component status, circuit states, dead-letter counts, and restart counts. Kubernetes liveness/readiness probes should use this endpoint.
3. **Forwards alerts.** A listener registered on the system topic (via the `:register-listener` mechanism in `app.clj`) can forward `:fatal`, `:poison-pill`, `:circuit-open`, and `:dead-letter` events to an external alerting channel.
4. **Tracks aggregate health.** The system processor should maintain a derived health state in an atom: `:healthy` (all components running, no open circuits), `:degraded` (some components in error or circuits open, but core function continues), `:critical` (Kafka unavailable or a core processor halted). This state drives the HTTP health endpoint's response code.

### System event schema

All events published to the system topic should share a consistent shape under `::event/data`:

```clojure
{:type          keyword          ; :kafka-exception, :dead-letter, :circuit-open, :effect-timeout, ...
 :severity      keyword          ; :info, :warn, :error, :fatal
 :source        keyword          ; component name
 :entity-type   type             ; component record type
 :timestamp     long             ; System/currentTimeMillis
 :message       string           ; human-readable description
 ;; type-specific fields follow
 }
```

The `p/log-event` multimethod (dispatching on `[:genegraph.framework.event/data :type]`) in `app.clj` is the right extension point for type-specific handling. Add `defmethod p/log-event` implementations for each error type.

---

## Summary of Recommended Additions to the Framework

| Addition | Addresses |
|---|---|
| `kafka/exception-outcome` wired to actual backoff/restart behavior | Kafka unavailability |
| Restart budget tracking in component state atoms | Kafka unavailability |
| Error-capture interceptor for all chains | Programming errors |
| Dead-letter store (RocksDB table + HTTP endpoint) | Programming errors, API failures |
| Per-offset poison-pill detection and offset advancement | Programming errors |
| Reduced default `::event/effect-timeout` (60 s) | Programming errors |
| Circuit breaker state atom per external dependency | API failures |
| Mandatory timeout on all external HTTP calls | API failures |
| `:api-failure` / `:circuit-open` system events | API failures |
| Custom system processor template with health aggregation | All three |
| `/healthz` HTTP endpoint with structured component status | All three |
| Consistent system event schema enforced by spec | All three |
