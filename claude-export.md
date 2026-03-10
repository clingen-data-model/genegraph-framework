## genegraph-framework

This application is built on [genegraph-framework](https://github.com/clingen-data-model/genegraph-framework), a Clojure library for data-driven Kafka stream-processing apps. The entire application is described as a single map and started with `p/init` → `p/start`.

### Key concepts

- **App-def map** — top-level keys: `:kafka-clusters`, `:topics`, `:storage`, `:processors`, `:http-servers`. Components reference each other by keyword; `p/init` wires them together.
- **Events** — plain maps with `::event/` namespaced keys (`::event/data`, `::event/key`, `::event/offset`, etc.). Interceptors receive and return event maps.
- **Effects are deferred** — `event/store`, `event/delete`, `event/publish` accumulate on the event map and execute *after* the interceptor chain completes.
- **Interceptors** — Pedestal interceptors (`:enter` fn receives event map, returns modified event map). Specified as values or fully-qualified symbols in the `:interceptors` vector.

### Component types

| Kind | Types |
|---|---|
| Topics | `:simple-queue-topic`, `:kafka-consumer-group-topic`, `:kafka-producer-topic`, `:kafka-reader-topic` |
| Processors | `:processor` (single-threaded), `:parallel-processor` (3-stage pipeline; `:gate-fn` for per-key ordering) |
| Storage | `:rocksdb` (Nippy/LZ4), `:rdf` (Jena TDB2/SPARQL; use `rdf/tx` for reads), `:gcs-bucket`, `:atom` |
| HTTP | `:http-server` (Pedestal/http-kit; `:endpoints` link routes to processors) |

### Startup / shutdown

Order: system-processor → storage → topics → processors → http-servers (shutdown is reverse). Every app has an internal `:system` `SimpleQueueTopic`; components publish lifecycle events (`:starting`, `:started`, `:up-to-date`, `:exception`) to it.

### Namespace imports

```clojure
(require '[genegraph.framework.protocol :as p]
         '[genegraph.framework.event :as event]
         '[genegraph.framework.storage :as storage]
         '[genegraph.framework.storage.rdf :as rdf]
         '[genegraph.framework.kafka.admin :as kafka-admin]
         '[io.pedestal.interceptor :as interceptor])
```
