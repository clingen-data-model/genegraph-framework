# genegraph-framework

A Clojure framework for building stream-processing applications with Kafka, persistent storage, and HTTP APIs. It provides a declarative, data-driven model for wiring together event sources, processing pipelines, storage backends, and HTTP endpoints into a cohesive application.

---

## Table of Contents

- [Overview](#overview)
- [Core Concepts](#core-concepts)
- [Application Structure](#application-structure)
- [Topics](#topics)
- [Processors](#processors)
- [Storage](#storage)
- [HTTP Servers](#http-servers)
- [Events](#events)
- [Kafka Configuration](#kafka-configuration)
- [Lifecycle Management](#lifecycle-management)
- [Configuration Reference](#configuration-reference)
- [Examples](#examples)

---

## Overview

Genegraph-framework applications are defined as a single map describing all their components — Kafka topics, processors, storage backends, and HTTP servers — which is then initialized and started as a unit. The framework handles wiring, lifecycle ordering, and cross-cutting concerns like offset tracking and transactional publishing.

The primary entry points are:

```clojure
(require '[genegraph.framework.app :as app]
         '[genegraph.framework.protocol :as p]
         '[genegraph.framework.event :as event])

;; Define the app
(def my-app-def
  {:type :genegraph-app
   :topics   { ... }
   :storage  { ... }
   :processors { ... }
   :http-servers { ... }})

;; Initialize (wires refs, creates records)
(def my-app (p/init my-app-def))

;; Start all components
(p/start my-app)

;; Stop all components (in reverse order)
(p/stop my-app)
```

---

## Core Concepts

### Protocol-driven design

All major components implement a small set of protocols defined in `genegraph.framework.protocol`:

| Protocol | Methods | Purpose |
|---|---|---|
| `Lifecycle` | `start`, `stop` | Manage component lifecycle |
| `Resetable` | `reset` | Reset component to initial state |
| `Consumer` | `poll` | Pull the next event from a topic |
| `Publisher` | `publish` | Write an event to a topic |
| `TransactionalPublisher` | `send-offset`, `commit` | Kafka transactional publishing |
| `Offsets` | `set-offset!` | Set the starting read offset for a topic |
| `EventProcessor` | `process` | Process a single event |
| `StatefulInterceptors` | `as-interceptors` | Represent a processor as an interceptor chain |

### `p/init` multimethod

Every component type is created via the `p/init` multimethod, which dispatches on `:type`. The app initialization calls `p/init` on each declared component:

```clojure
(p/init {:type :kafka-consumer-group-topic, ...})  ; → KafkaConsumerGroupTopic
(p/init {:type :processor, ...})                    ; → Processor
(p/init {:type :rocksdb, ...})                      ; → RocksDBInstance
```

### System topic

Every app has an internal `:system` topic (a `SimpleQueueTopic`). Components publish lifecycle events and exceptions to it. A *system processor* subscribes to `:system` and handles these events — by default it logs them and dispatches completion promises registered by listeners.

---

## Application Structure

A `genegraph-app` definition supports these top-level keys:

| Key | Type | Description |
|---|---|---|
| `:type` | keyword | Must be `:genegraph-app` |
| `:kafka-clusters` | map | Named Kafka cluster configurations |
| `:topics` | map | Named topic definitions |
| `:storage` | map | Named storage backend definitions |
| `:processors` | map | Named processor definitions |
| `:http-servers` | map | Named HTTP server definitions |

References between components use keywords: a processor's `:subscribe :my-topic` refers to the topic named `:my-topic` in `:topics`, and `:kafka-cluster :my-cluster` refers to the cluster in `:kafka-clusters`.

**Startup order:**
1. System processor
2. Storage backends
3. Topics
4. Non-system processors
5. HTTP servers

**Shutdown order** is the reverse.

---

## Topics

Topics are event channels. There are four topic types.

### `:simple-queue-topic`

An in-process `ArrayBlockingQueue`. Useful for testing and for routing events between processors within the same JVM.

```clojure
{:name :my-queue
 :type :simple-queue-topic
 :buffer-size 10   ; optional, default 10
 :timeout 1000}    ; poll timeout in ms, default 1000
```

### `:kafka-consumer-group-topic`

Reads from a Kafka topic using a consumer group. This is the standard topic type for consuming persistent Kafka streams. It maintains offsets via a consumer group and can optionally use a local backing store to resume from a local snapshot.

```clojure
{:name :my-consumer-topic
 :type :kafka-consumer-group-topic
 :kafka-cluster :my-cluster          ; reference to a kafka-cluster definition
 :kafka-topic "my-kafka-topic"       ; the Kafka topic name (string)
 :kafka-consumer-group "my-cg"       ; Kafka consumer group ID
 :serialization :json                ; :json, :edn, or an RDF format keyword
 :create-producer true               ; create a producer for this topic (for transactional publishing)
 :buffer-size 100                    ; in-memory queue size
 :timeout 1000                       ; poll timeout in ms
 :kafka-options {}                   ; additional Kafka consumer config overrides
 :reset-opts {}}                     ; options for p/reset (e.g. {:clear-topic true})
```

**How it works:** On start, the topic checks whether the local backing store (if any) lags behind the consumer group. If it does, it replays events from Kafka directly into the queue with `:skip-publish-effects true` to rebuild local state before resuming normal processing. It delivers a `:up-to-date` system event once processing has caught up to the end offset at startup.

### `:kafka-producer-topic`

A write-only topic that publishes events to Kafka. Does not consume.

```clojure
{:name :my-output-topic
 :type :kafka-producer-topic
 :kafka-cluster :my-cluster
 :kafka-topic "my-output-kafka-topic"
 :create-producer true}
```

### `:kafka-reader-topic`

Reads from a Kafka topic starting from a local offset, without a consumer group. Suitable for reading reference/base data that is not shared with other consumers. All events are published with `:skip-publish-effects true`.

```clojure
{:name :my-base-topic
 :type :kafka-reader-topic
 :kafka-cluster :my-cluster
 :kafka-topic "my-base-kafka-topic"
 :serialization :json}
```

---

## Processors

Processors consume events from a subscribed topic, run them through an interceptor chain, and execute effects (publishing to other topics, writing to storage).

### `:processor`

A single-threaded processor. Events are processed sequentially.

```clojure
{:name :my-processor
 :type :processor
 :subscribe :my-topic                     ; topic keyword to consume from
 :kafka-cluster :my-cluster               ; required if using a Kafka-backed topic
 :kafka-transactional-id "my-tx-id"       ; enable Kafka transactions (optional)
 :backing-store :my-rocksdb               ; storage key to track offsets (optional)
 :storage {:my-rocksdb :my-rocksdb-ref}  ; storage backends available in events
 :interceptors [my-interceptor-1
                my-interceptor-2]
 :init-fn (fn [p] p)}                     ; optional init hook, runs before start
```

### `:parallel-processor`

A three-stage pipelined processor: deserialization, processing, and effect execution each run on their own thread, enabling concurrency. An optional `:gate-fn` serializes events that share a key, preserving ordering within a key while allowing parallelism across keys.

```clojure
{:name :my-parallel-processor
 :type :parallel-processor
 :subscribe :my-topic
 :gate-fn :key                  ; fn applied to ::event/data to derive a serialization key
 :parallel-gate-timeout 30      ; seconds to wait for a gate before erroring (default 30)
 :interceptors [my-interceptor]}
```

### Interceptors

Processors delegate business logic to Pedestal interceptors. Interceptor `:enter` functions receive the event map and return it (optionally modified). Use `event/store`, `event/delete`, and `event/publish` to accumulate deferred side effects:

```clojure
(require '[io.pedestal.interceptor :as interceptor]
         '[genegraph.framework.event :as event]
         '[genegraph.framework.storage :as storage])

(def my-interceptor
  (interceptor/interceptor
   {:name ::my-interceptor
    :enter (fn [e]
             (let [data (::event/data e)
                   k    (::event/key e)]
               ;; Queue a deferred write to storage
               (-> e
                   (event/store :my-rocksdb k data)
                   ;; Queue a deferred publish to another topic
                   (event/publish {::event/topic :output-topic
                                   ::event/key k
                                   ::event/data (transform data)}))))}))
```

Effects are accumulated in the event map and executed after the interceptor chain completes. Storage writes and topic publishes are committed atomically within a Kafka transaction when `:kafka-transactional-id` is configured.

#### Available context in events

When an interceptor receives an event, the following keys are available:

| Key | Description |
|---|---|
| `::event/data` | Deserialized event payload |
| `::event/key` | Record key |
| `::event/value` | Raw (serialized) value |
| `::event/offset` | Kafka offset (for Kafka-backed topics) |
| `::event/topic` | Topic name keyword |
| `::event/kafka-topic` | Kafka topic string |
| `::event/format` | Serialization format (`:json`, `:edn`, RDF keyword) |
| `::event/timestamp` | Record timestamp |
| `::event/source` | `:kafka` or nil |
| `::storage/storage` | Map of instantiated storage backends |
| `::event/topics` | Map of available topics for publishing |
| `::event/producer` | Kafka producer (for transactional publishing) |
| `::event/completion-promise` | Promise delivered when all effects complete |
| `::event/skip-publish-effects` | When true, publish effects are skipped (used during replay) |

#### Symbols as interceptors

Interceptors may be specified as fully-qualified symbols in the `:interceptors` vector. They are resolved at processor start time, enabling hot-reloading in development:

```clojure
:interceptors ['my.ns/my-interceptor-fn]
```

---

## Storage

### `:rocksdb`

An embedded key-value store backed by RocksDB (via `org.rocksdb`). Values are serialized with Nippy. Supports snapshot/restore to/from an archive file.

```clojure
{:type :rocksdb
 :name :my-rocksdb
 :path "/data/my-rocksdb"
 :snapshot-handle {:type :file          ; or :gcs
                   :base "/data"
                   :path "snapshot.tar.lz4"}
 :load-snapshot true                    ; restore from snapshot on start if no local data
 :reset-opts {:destroy-snapshot true}}  ; used by p/reset
```

**Key types:** RocksDB keys are derived from Clojure values:

| Clojure type | Behavior |
|---|---|
| `bytes` | Used as-is |
| `String` | 64-bit XX3 hash |
| `Long` | Big-endian 8 bytes (ordered, supports range scans) |
| `Keyword` | Same as String |
| `Sequential` | Concatenation of each element hashed to 8 bytes |

**Operations (on the raw `RocksDB` instance via `storage/instance`):**

```clojure
(storage/write  db :my-key {:my :value})
(storage/read   db :my-key)            ; returns ::storage/miss if absent
(storage/delete db :my-key)
(storage/scan   db :prefix)            ; prefix scan
(storage/scan   db start end)          ; range scan
(storage/range-delete db :prefix)
```

### `:rdf`

An Apache Jena TDB2 triplestore. Supports SPARQL queries, RDF models, and snapshot/restore.

```clojure
{:type :rdf
 :name :my-jena
 :path "/data/my-jena"
 :snapshot-handle {:type :gcs
                   :bucket "my-bucket"
                   :path "snapshot.nq.gz"}
 :load-snapshot true}
```

Access the Jena `Dataset` via `(storage/instance my-rdf-store)`. The `rdf/tx` macro wraps reads in a transaction:

```clojure
(require '[genegraph.framework.storage.rdf :as rdf])

(rdf/tx dataset
  (rdf/resource :dc/Agent dataset))
```

RDF event serialization/deserialization is supported for formats `:rdf/rdf-xml`, `:rdf/json-ld`, `:rdf/turtle`, `:rdf/n-triples`.

### `:gcs-bucket`

Google Cloud Storage bucket. Value reads return `InputStream`; writes accept `InputStream`.

```clojure
{:type :gcs-bucket
 :name :my-bucket
 :bucket "my-gcs-bucket-name"}
```

Access via `(storage/instance my-gcs)`, then:

```clojure
(storage/write @instance "path/in/bucket" input-stream)
(storage/read  @instance "path/in/bucket")   ; returns InputStream
(storage/scan  @instance "prefix/")          ; returns seq of Blob objects
```

### `:atom`

A simple in-memory atom-backed store. Useful for testing.

### Snapshots

Storage backends that implement `storage/Snapshot` support:

```clojure
(storage/store-snapshot my-store)    ; write snapshot to configured snapshot-handle
(storage/restore-snapshot my-store)  ; restore snapshot from snapshot-handle
```

RocksDB snapshots are LZ4-compressed tar archives; Jena TDB snapshots are gzipped N-Quads files.

### Handles

A *handle* is a pointer to a file or GCS object used as a snapshot target:

```clojure
{:type :file  :base "/data"  :path "snapshot.tar.lz4"}
{:type :gcs   :bucket "my-bucket"  :path "snapshot.nq.gz"}
```

Use `storage/as-handle` to get a Java IO-compatible object from a handle map.

---

## HTTP Servers

HTTP servers are built on Pedestal and http-kit. Endpoints are linked to processors via keyword references.

```clojure
{:type :http-server
 :name :my-server
 :port 8888
 :host "0.0.0.0"
 :endpoints [{:path "/api/resource"
              :method :get              ; default :get
              :processor :my-api-processor}]
 :routes [["/ready" :get (fn [_] {:status 200 :body "ok"}) :route-name ::ready]
          ["/live"  :get (fn [_] {:status 200 :body "ok"}) :route-name ::live]]}
```

`:endpoints` connect HTTP routes to named processors. The processor's interceptor chain handles the request and must assoc a `:response` map onto the context.

`:routes` accepts raw Pedestal route vectors for lightweight handlers that do not need a full processor.

The processor referenced in an endpoint must be defined in `:processors` and implement `StatefulInterceptors` (all framework processor types do).

---

## Events

Events are plain Clojure maps. The framework uses namespaced keys under `::event/` (i.e., `genegraph.framework.event/`).

### Serialization / Deserialization

`event/deserialize` and `event/serialize` are multimethods dispatching on `::event/format`:

| Format | Description |
|---|---|
| `:json` | JSON via charred |
| `:edn` | EDN via `clojure.edn` |
| `::rdf/turtle`, `::rdf/json-ld`, `::rdf/rdf-xml`, `::rdf/n-triples` | RDF via Apache Jena |
| default | Pass-through (no transformation) |

### Effects

Effects are queued onto the event during interceptor processing and executed after the chain completes:

```clojure
;; Queue a storage write
(event/store event :storage-key "my-key" {:my :data})

;; Queue a storage delete
(event/delete event :storage-key "my-key")

;; Queue a publish to another topic
(event/publish event {::event/topic  :output-topic
                      ::event/key    "my-key"
                      ::event/data   {:my :data}
                      ::event/format :json})
```

### Completion promises

Every event has a `::event/completion-promise`. It is delivered `true` once all effects succeed, `false` if any effect fails, and `:timeout` if effects take longer than `::event/effect-timeout` (default: 1 hour). This lets callers await confirmation that an event has been fully processed:

```clojure
(p/publish my-topic my-event)
@(::event/completion-promise my-event)   ; blocks until complete
```

---

## Kafka Configuration

### Cluster definition

```clojure
{:type :kafka-cluster
 :common-config  {"bootstrap.servers" "localhost:9092"
                  ;; TLS / SASL options go here
                  }
 :consumer-config {"key.deserializer"   "...StringDeserializer"
                   "value.deserializer" "...StringDeserializer"}
 :producer-config {"key.serializer"     "...StringSerializer"
                   "value.serializer"   "...StringSerializer"}}
```

For Confluent Cloud, include SASL/SSL settings in `:common-config`.

### Kafka admin

`genegraph.framework.kafka.admin` provides utilities for provisioning topics:

```clojure
(require '[genegraph.framework.kafka.admin :as kafka-admin])

;; Review planned admin actions
(kafka-admin/admin-actions-by-cluster my-app)

;; Apply them
(kafka-admin/configure-kafka-for-app! my-app)
```

### Transactional publishing

Set `:kafka-transactional-id` on a processor to enable exactly-once semantics. The processor's Kafka producer will wrap publishes and offset commits in a single transaction:

```clojure
{:type :processor
 :kafka-transactional-id "my-unique-tx-id"
 ...}
```

### Exception handling

The Kafka layer categorizes exceptions into three groups (defined in `genegraph.framework.kafka`):

- **ignorable** — Wakeup, Interrupt, RebalanceInProgress: silently swallowed
- **fatal** — Auth, invalid config/topic/offset, illegal state: halt processing
- **restartable** — ProducerFenced, Timeout, CommitFailed, generic KafkaException: attempt restart

---

## Lifecycle Management

### `p/reset`

Resets storage and topics to their initial state. Behavior depends on `:reset-opts`:

- **Topics**: Resets consumer group offsets to the beginning. `{:clear-topic true}` deletes and recreates the Kafka topic.
- **Storage**: Destroys local data. `{:destroy-snapshot true}` also deletes the upstream snapshot.

```clojure
;; Reset entire app (all resetable components)
(p/reset my-app)

;; Reset individual component
(p/reset (get-in my-app [:storage :my-rocksdb]))
```

### System events

Components publish structured maps to the `:system` topic at lifecycle milestones:

```clojure
{:source :my-topic
 :entity-type KafkaConsumerGroupTopic
 :state :starting | :started | :up-to-date | :exception | :restoring-snapshot | ...}
```

The default system processor logs these. You can replace it with a custom processor by defining one with `:subscribe :system` in your `:processors` map.

### Up-to-date detection

Kafka-backed consumer topics publish a `:up-to-date` system event when they have processed all records that existed at startup. This is used for initialization sequencing in applications that need to build local state before serving requests.

---

## Event Store

`genegraph.framework.event.store` provides utilities for saving and replaying event sequences to/from gzipped EDN files:

```clojure
(require '[genegraph.framework.event.store :as event-store])

;; Write events to file
(event-store/with-event-writer [w "/data/events.edn.gz"]
  (run! prn my-events))

;; Read events from file
(event-store/with-event-reader [r "/data/events.edn.gz"]
  (doall (event-store/event-seq r)))

;; Copy a Kafka topic to a file
(genegraph.framework.kafka/topic->event-file my-topic-def "/data/events.edn.gz")
```

---

## Configuration Reference

### Full app skeleton

```clojure
{:type :genegraph-app

 :kafka-clusters
 {:my-cluster
  {:type :kafka-cluster
   :common-config  {"bootstrap.servers" "..."}
   :consumer-config {"key.deserializer" "..." "value.deserializer" "..."}
   :producer-config {"key.serializer"   "..." "value.serializer"   "..."}}}

 :storage
 {:my-rocksdb {:type :rocksdb  :name :my-rocksdb  :path "/data/rocks"}
  :my-rdf     {:type :rdf      :name :my-rdf      :path "/data/jena"}
  :my-gcs     {:type :gcs-bucket :name :my-gcs    :bucket "my-bucket"}}

 :topics
 {:input-topic
  {:name :input-topic
   :type :kafka-consumer-group-topic
   :kafka-cluster :my-cluster
   :kafka-topic "my-kafka-topic"
   :kafka-consumer-group "my-cg"
   :serialization :json}

  :output-topic
  {:name :output-topic
   :type :kafka-producer-topic
   :kafka-cluster :my-cluster
   :kafka-topic "my-output-topic"}

  :internal-queue
  {:name :internal-queue
   :type :simple-queue-topic}}

 :processors
 {:my-processor
  {:name :my-processor
   :type :processor
   :subscribe :input-topic
   :kafka-cluster :my-cluster
   :backing-store :my-rocksdb
   :storage {:my-rocksdb :my-rocksdb}
   :interceptors [my-interceptor]}}

 :http-servers
 {:api-server
  {:type :http-server
   :name :api-server
   :port 8888
   :endpoints [{:path "/api" :method :get :processor :api-processor}]
   :routes [["/ready" :get (fn [_] {:status 200 :body "ok"}) :route-name ::ready]]}}}
```

---

## Examples

The `example/` directory contains runnable examples:

| File | Description |
|---|---|
| `ccloud_example.clj` | Basic Confluent Cloud publish/consume |
| `app_lifecycle_example.clj` | Full app with RocksDB, Jena, snapshots, and multiple topics |
| `http_init_example.clj` | HTTP server with processor-backed endpoints |
| `parallel_processor.clj` | Parallel processor with gate-fn for per-key ordering |
| `transaction_example.clj` | RDF triplestore storage with `rdf/tx` |
| `transaction_publish_example.clj` | Kafka transactional publishing |
| `effect_completion_example.clj` | Awaiting event completion promises |
| `event_error_example.clj` | Error handling in event processing |
| `lucene_example.clj` | Full-text search integration |
| `control_plane_example.clj` | Using the system topic for control-plane events |

### Minimal example

```clojure
(require '[genegraph.framework.app :as app]
         '[genegraph.framework.protocol :as p]
         '[genegraph.framework.event :as event]
         '[io.pedestal.interceptor :as interceptor])

(def echo-interceptor
  (interceptor/interceptor
   {:name ::echo
    :enter (fn [e]
             (println "received:" (::event/data e))
             e)}))

(def my-app-def
  {:type :genegraph-app
   :topics {:events {:type :simple-queue-topic :name :events}}
   :processors {:echo {:type :processor
                       :name :echo
                       :subscribe :events
                       :interceptors [echo-interceptor]}}})

(def my-app (p/init my-app-def))
(p/start my-app)

(p/publish (get-in my-app [:topics :events])
           {::event/key "k1" ::event/data {:hello "world"}})

(p/stop my-app)
```
