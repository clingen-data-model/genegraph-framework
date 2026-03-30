# Google Cloud Pub/Sub Topic Design

This document describes the design for adding Google Cloud Pub/Sub topic types to the genegraph-framework, following the same patterns used by the existing Kafka topic types.

---

## Overview

Google Cloud Pub/Sub is a managed messaging service with a pull/push delivery model. It differs from Kafka in several important ways that affect the design:

| Concern | Kafka | Pub/Sub |
|---|---|---|
| Replay | Seek to any offset | No replay; messages deleted after ACK or retention window |
| Ordering | Per-partition sequential | Ordered only with ordering keys (opt-in) |
| Consumer model | Consumer groups; offset committed to broker | Subscriptions; per-message ACK |
| At-least-once | Yes | Yes (exactly-once delivery available) |
| Offsets | Numeric, monotonic | No numeric offset; message ID is opaque |
| Backlog catch-up | Seek to beginning | Snapshot + seek, or replay from earliest retained message |
| Transactional publish | Kafka transactions | Not supported natively |

Because Pub/Sub does not expose numeric offsets, the `p/Offsets` protocol and the backing-store offset reconciliation logic used by `KafkaConsumerGroupTopic` and `KafkaReaderTopic` do not apply. The `:up-to-date` signal should fire immediately after draining the current backlog (i.e., after a pull returns 0 messages).

---

## New Topic Types

Two new topic types are proposed, mirroring the Kafka split between consumer and producer:

| Type keyword | Protocols | Description |
|---|---|---|
| `:pubsub-consumer-topic` | `Lifecycle`, `Consumer`, `Publisher`, `Status` | Pulls messages from a Pub/Sub subscription; ACKs after successful processing |
| `:pubsub-producer-topic` | `Lifecycle`, `Publisher`, `Status` | Publishes messages to a Pub/Sub topic |

A combined type is not proposed. Pub/Sub separates topics (write) from subscriptions (read), so the asymmetry is inherent.

---

## Configuration

### PubsubConsumerTopic

```clojure
{:type                  :pubsub-consumer-topic
 :name                  :my-consumer            ; framework component name (keyword)
 :pubsub-project        "my-gcp-project"        ; GCP project ID
 :pubsub-subscription   "my-subscription"       ; subscription name (not full path)
 :serialization         :json                   ; passed to event/deserialize
 :buffer-size           100                     ; internal queue capacity
 :timeout               1000                    ; poll timeout ms
 :max-messages          100                     ; max messages per pull RPC
 :ack-deadline-seconds  60                      ; how long before unACKed message redelivered
 :create-producer       false}                  ; whether to create a publisher for nack/republish
```

### PubsubProducerTopic

```clojure
{:type               :pubsub-producer-topic
 :name               :my-producer              ; framework component name
 :pubsub-project     "my-gcp-project"
 :pubsub-topic       "my-topic"               ; topic name (not full path)
 :serialization      :json}
```

Credentials are resolved from the standard GCP Application Default Credentials (ADC) chain — environment variable `GOOGLE_APPLICATION_CREDENTIALS`, workload identity, or the metadata server — consistent with how the existing `:gcs-bucket` storage backend works.

---

## Internal Architecture

### PubsubConsumerTopic

The consumer runs a background thread that loops over synchronous pull RPCs. Pulled messages are placed onto an `ArrayBlockingQueue` (same pattern as `KafkaConsumerGroupTopic`). The processor calls `p/poll` to dequeue one event at a time. ACK/NACK is deferred until after the interceptor chain runs, using the same `::event/completion-promise` mechanism.

```
pull thread
  └── SubscriberStubClient.pull(subscription, maxMessages)
        └── for each message:
              ├── convert to event map (message → ::event/value, attributes → ::event/metadata)
              ├── attach ::event/ack-id  (needed for ACK/NACK)
              ├── attach ::event/completion-promise
              └── .put(queue)

processor thread
  └── p/poll → .poll(queue, timeout, MILLISECONDS)
        └── interceptor chain
              └── event/deserialize, user interceptors, effects
        └── on completion-promise delivery:
              ├── true  → ACK (subscriber.acknowledge [ack-id])
              └── false → NACK (subscriber.modifyAckDeadline [ack-id] 0)
```

The ACK/NACK callback runs in a separate virtual thread to avoid blocking the processor.

#### Up-to-date signal

Since there are no numeric offsets, the up-to-date signal fires after the first pull RPC that returns zero messages. This is analogous to catching up to the end offset in Kafka. A `:delivered-up-to-date-event?` flag in the state atom (same as the Kafka topics) prevents duplicate delivery.

#### State atom

```clojure
{:status                    :stopped     ; :stopped | :starting | :running | :restarting | :exception
 :delivered-up-to-date?     false
 :restarts                  0
 :restart-timestamps        []
 :pubsub-client             nil          ; SubscriberStubClient or nil
 :last-acked-message-id     nil}
```

### PubsubProducerTopic

The producer wraps a `Publisher` client (from the Google Cloud Pub/Sub Java client library). Publishing is synchronous from the caller's perspective: `p/publish` serializes the event, calls `publisher.publish(PubsubMessage)`, and delivers the `commit-promise` in the ApiFuture callback.

```
p/publish (caller thread)
  └── event/serialize
  └── PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(value))...build()
  └── publisher.publish(message)   → ApiFuture<String>
        └── ApiFutures.addCallback(future,
              onSuccess: deliver(commit-promise, true)
              onFailure: deliver(commit-promise, false), log error)
```

---

## Event Map Keys

New keys in the `genegraph.framework.event` namespace:

| Key | Type | Description |
|---|---|---|
| `::event/pubsub-message-id` | String | Pub/Sub server-assigned message ID |
| `::event/pubsub-ack-id` | String | ACK ID for the pulled message (consumer only; not published downstream) |
| `::event/pubsub-attributes` | Map | Pub/Sub message attributes (string→string) |
| `::event/pubsub-publish-time` | Timestamp | Server publish timestamp |
| `::event/pubsub-topic` | String | Pub/Sub topic name (producer side) |
| `::event/pubsub-subscription` | String | Pub/Sub subscription name (consumer side) |

The `::event/value` key carries the raw message data (bytes or string), consistent with how Kafka events use `::event/value`. Deserialization via `event/deserialize` populates `::event/data`, unchanged.

---

## Error Handling and Restart Strategy

Pub/Sub errors are classified similarly to Kafka errors. The Google Cloud Java client throws subtypes of `com.google.api.gax.rpc.ApiException`. The relevant categories:

| Exception type | Pub/Sub status code | Recommended action |
|---|---|---|
| `ApiException` with `isRetryable() == true` | `UNAVAILABLE`, `DEADLINE_EXCEEDED`, etc. | Retry with exponential backoff |
| `ApiException` with `isRetryable() == false` | `NOT_FOUND`, `PERMISSION_DENIED`, `INVALID_ARGUMENT` | Halt; these indicate misconfiguration |
| Any other `Exception` | — | Treat as retryable up to restart budget |

An `exception-outcome` function mirroring the one in `kafka.clj` will classify these:

```clojure
(defn exception-outcome [e]
  (condp instance? e
    com.google.api.gax.rpc.ApiException
    (if (.isRetryable e)
      {:outcome :retry-action    :severity :warn}
      {:outcome :halt-application :severity :fatal})
    {:outcome :restart-client :severity :warn}))
```

The restart budget and exponential backoff strategy (50 restarts per 60-second window, base 500 ms, max 30 s, 20% jitter) reuse the same logic as `kafka.clj` (`restart-budget-exceeded?`, `attempt-consumer-restart!`).

Unacked messages are automatically redelivered by Pub/Sub after the `ack-deadline-seconds` expires, so there is no need to re-enqueue them manually on consumer restart.

---

## Namespace and File Layout

New files:

```
src/genegraph/framework/pubsub.clj          ; PubsubConsumerTopic, PubsubProducerTopic records
src/genegraph/framework/pubsub/admin.clj    ; topic/subscription provisioning utilities (optional)
```

Mirrors the structure of:
```
src/genegraph/framework/kafka.clj
src/genegraph/framework/kafka/admin.clj
```

Dependencies to add to `deps.edn`:
```clojure
com.google.cloud/google-cloud-pubsub {:mvn/version "1.131.0"}
```

---

## Protocol Implementations

### PubsubConsumerTopic

```clojure
(defrecord PubsubConsumerTopic
    [name
     pubsub-project
     pubsub-subscription
     buffer-size
     timeout
     max-messages
     serialization
     ^ArrayBlockingQueue queue
     state]

  p/Lifecycle
  (start [this] ...)   ; start pull loop thread, system-update :starting / :started
  (stop  [this] ...)   ; set :status :stopped, close client

  p/Consumer
  (poll [this]
    (when-let [e (.poll queue timeout TimeUnit/MILLISECONDS)]
      (assoc e ::event/topic name)))

  p/Publisher
  ;; Noop — consumer topics do not publish; included so processors
  ;; that call p/publish on their source topic do not crash.
  (publish [this event] nil)

  p/Status
  (status [this]
    {:name           name
     :subscription   pubsub-subscription
     :status         (:status @state)
     :queue-size     (.size queue)
     :up-to-date     (:delivered-up-to-date? @state)
     :restarts       (:restarts @state)}))

(derive PubsubConsumerTopic :genegraph/topic)

(defmethod p/init :pubsub-consumer-topic [topic-def]
  (map->PubsubConsumerTopic
   (merge {:timeout 1000 :buffer-size 100 :max-messages 100}
          topic-def
          {:queue (ArrayBlockingQueue. (:buffer-size topic-def 100))
           :state (atom (initial-state))})))
```

### PubsubProducerTopic

```clojure
(defrecord PubsubProducerTopic
    [name
     pubsub-project
     pubsub-topic
     serialization
     state]

  p/Lifecycle
  (start [this] ...)   ; create Publisher client, swap state :running
  (stop  [this] ...)   ; publisher.shutdown(), awaitTermination

  p/Publisher
  (publish [this event]
    (let [msg (-> event event/serialize build-pubsub-message)]
      (publish-with-callback (:publisher @state) msg event)))

  p/Status
  (status [this]
    {:name        name
     :pubsub-topic pubsub-topic
     :status      (:status @state)}))

(derive PubsubProducerTopic :genegraph/topic)

(defmethod p/init :pubsub-producer-topic [topic-def]
  (map->PubsubProducerTopic
   (assoc topic-def :state (atom {:status :stopped}))))
```

---

## ACK/NACK Integration

The `::event/completion-promise` mechanism already carries the result of processing back to the topic layer in `KafkaConsumerGroupTopic` (via `handle-event-status-updates`). The same pattern applies here:

A status monitor thread watches `::event/completion-promise` for each delivered event. When the promise resolves:
- `true` (or any non-false, non-`:timeout` value) → ACK the message
- `false` → NACK the message (set deadline to 0, triggering immediate redelivery)
- `:timeout` → log a warning and NACK

The `::event/pubsub-ack-id` key carries the ACK ID needed by the Pub/Sub client. This key should be stripped before the event is published downstream (analogous to how internal Kafka fields are not forwarded).

---

## Up-to-Date Semantics

Pub/Sub does not expose a broker-side "end offset." The up-to-date signal should fire when:

1. The first pull RPC after startup returns zero messages, **and**
2. At least one pull RPC has already returned messages (to avoid declaring up-to-date immediately on an empty subscription at startup).

If the subscription is empty at startup (first pull returns zero messages), up-to-date fires immediately — this is the correct behavior, as there is no backlog to process.

This is simpler than the Kafka implementation and does not require an `initial-consumer-group-offset` promise.

---

## What Is Not Supported

- **Transactional publishing**: Pub/Sub does not support Kafka-style transactions linking consumer offset commits to publisher sends. Exactly-once semantics must be achieved at the application level (e.g., idempotent writes to RocksDB keyed by `pubsub-message-id`).
- **Offset-based replay**: There is no `p/Offsets` implementation. Applications that need replay must use Pub/Sub snapshots externally or store events in a separate log.
- **Ordered delivery**: Ordering keys are an opt-in Pub/Sub feature. If needed, set `::event/key` on published messages and configure the subscription for ordered delivery; the framework does not enforce this.
- **Backing-store offset reconciliation**: The `produce-local-events` / `update-local-storage!` pattern from `KafkaConsumerGroupTopic` is not applicable. Applications that maintain local state should use message ID as the idempotency key.

---

## Example App-Def Fragment

```clojure
{:type :genegraph-app
 :topics
 {:my-input
  {:type                :pubsub-consumer-topic
   :pubsub-project      "my-project"
   :pubsub-subscription "my-sub"
   :serialization       :json
   :buffer-size         50}

  :my-output
  {:type            :pubsub-producer-topic
   :pubsub-project  "my-project"
   :pubsub-topic    "my-output-topic"
   :serialization   :json}}

 :processors
 {:my-processor
  {:type      :processor
   :subscribe :my-input
   :interceptors [my-interceptor]
   :publish-to [:my-output]}}}
```

---

## Open Questions

1. **Streaming vs. pull**: The Google Cloud Java client offers both a `Subscriber` (streaming, push-based, manages its own threads) and a `SubscriberStubClient` (explicit pull). The design above uses explicit pull to keep the threading model consistent with the Kafka consumer loop. The streaming `Subscriber` may offer lower latency at the cost of less control over backpressure. This should be evaluated.

2. **Message ordering keys**: If the application needs ordered delivery, the `:gate-fn` mechanism in `ParallelProcessor` can serialize events with the same key. The framework does not need to change; the application sets ordering keys on messages. Document this pattern in examples.

3. **Dead-letter topics**: Pub/Sub has native dead-letter topic support at the subscription level. Should the framework expose a `:dead-letter-topic` configuration key and wire it to the subscription's `DeadLetterPolicy`, or leave DLT configuration to infrastructure? Recommendation: leave it to infrastructure (Terraform/gcloud), but document the pattern.

4. **Emulator support for testing**: The Pub/Sub emulator (`gcloud beta emulators pubsub`) can be used in tests analogously to the embedded Kafka used in existing tests. A test helper namespace `genegraph.framework.pubsub.test-support` could manage emulator lifecycle. This is deferred to the implementation phase.
