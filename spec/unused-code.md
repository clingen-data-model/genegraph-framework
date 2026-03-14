# Unused Code Report: Genegraph Framework

This report identifies code that is defined but never called within the production source tree (`src/`). Findings are grouped by severity. All line numbers are stable as of the review.

---

## Summary

| Category | Count |
|---|---|
| Dead functions / vars in production namespaces | 12 |
| Entire namespaces with no callers | 2 |
| Top-level expressions with side effects | 4 |
| Duplicate definitions | 1 |
| Disabled code (`#_`) leaving stubs | 3 |

---

## Category 1: Dead Functions and Vars in Production Namespaces

These symbols are defined in production source files but are never called by any other production code. They also have no `defmethod` implementations that would dispatch to them, no references via `var-get`, and do not appear in any `:require` or `:import` of another namespace.

### 1.1 `vec->key-bytes` — `storage/rocksdb.clj:20`

```clojure
(defn vec->key-bytes [x]
  (println "vector")
  (let [bs (ByteBuffer/allocate (* 8 (count x)))]
    (run! #(.putLong bs (k->long %)) x)
    (.toByteArray bs)))
```

Never called. The `Sequential` key-encoding extension at line 60 duplicates this logic inline. Contains a live `println` debug statement. The `KeyBytes` protocol extension for `Sequential` (lines 60–65) is what actually runs; this function is an orphaned predecessor.

### 1.2 `get-or-create-gate` — `processor.clj:274`

```clojure
(defn get-or-create-gate [state event k]
  (if-let [gate (get (:gates @state) k)]
    gate
    (let [new-gate (Semaphore. 1 true)]
      (swap! state assoc-in :gates k new-gate)  ; also has a bug: non-vector path
      new-gate)))
```

Never called. `process-gated-parallel-event` (line 294) reads gates directly via `(get (:gates @state) k)` without calling this function. Its existence implies an earlier design where gate creation was factored out. Also has a bug: `assoc-in` receives `:gates` (a keyword, not a vector) as the path argument.

### 1.3 `restart-kafka-producer!` — `kafka.clj:798`

```clojure
(defn restart-kafka-producer! [{:keys [state] :as genegraph-producer}]
  (swap! state :status :restarting)
  (.close (:producer @state)))
```

Never called. Also has the `swap!`-missing-`assoc` bug (passes `:status` as the function argument). The comment above it (`restart-kafka-producer!`) suggests it was intended as the recovery path for `restartable-exceptions`, but the recovery logic was never wired into the main exception handler.

### 1.4 `execute-async` and `execute-sync` — `storage/rdf/instance.clj:205` and `211`

```clojure
(defn execute-async [...]  ; line 205
(defn execute-sync [...]   ; line 211
```

Neither is called anywhere. `execute-async` was presumably an alternative to the internal write-loop queue; `execute-sync` was presumably a testing or migration helper. The actual write path goes through the `write-loop` thread directly.

### 1.5 `add-model` multimethod — `event.clj:8`

```clojure
(defmulti add-model ::format)
```

Declared but never has any `defmethod` implementations in any namespace, and is never invoked anywhere. Not to be confused with the `deserialize` and `serialize` multimethods, which are implemented and used.

### 1.6 `in-exception-category?` and `exception->error-map` — `kafka.clj:79` and `91`

```clojure
(defn in-exception-category? [e category] ...)  ; line 79
(defn exception->error-map [e] ...)              ; line 91
```

Both are defined but appear only inside `comment` blocks. `exception->error-map` calls `exception-outcome` (which is production code), but `exception->error-map` itself is never called from production code.

### 1.7 `ignorable-exceptions`, `fatal-exceptions`, `restartable-exceptions` — `kafka.clj:50–77`

```clojure
(def ignorable-exceptions [...])    ; line 50
(def fatal-exceptions [...])        ; line 55
(def restartable-exceptions [...])  ; line 67
```

Three category-lists never used in production code. They would pair with `in-exception-category?` (itself unused, see 1.6). The actual exception dispatch uses `exception-outcome` (line 84), which pattern-matches on type directly without these lists.

### 1.8 `acl-binding->map` — `kafka/admin.clj:112`

```clojure
(defn acl-binding->map [binding]
  (let [entry (.entry binding)
        pattern (.pattern binding)]
    {:principal (.principal entry) ...}))
```

Never called in production code. It converts a Kafka `AclBinding` object to a Clojure map for human inspection, but no code actually calls it. Likely created during admin tooling development for REPL use.

### 1.9 `delete-topic`, `delete-acls`, `delete-acls-for-user` — `kafka/admin.clj:71`, `85`, `91`

These three functions are only called inside `comment` blocks. `delete-acls` is only called by `delete-acls-for-user`, which is only called in a `comment`. These are manual admin helpers that never became part of the `configure-kafka-for-app!` workflow.

---

## Category 2: Entire Namespaces with No Production Callers

Neither of these namespaces is `:require`d by any other namespace in `src/`.

### 2.1 `genegraph.framework.id` — `src/genegraph/framework/id.clj`

A complete IRI generation system for content-addressed identifiers using XX3 hashing and Base64 encoding. Provides `register-type`, `iri`, `random-iri`, and the `HashablePrimitive` protocol with extensions for `String`, `Long`, `Keyword`, `PersistentVector`, and `IPersistentMap`.

Not required by any other namespace. All usage appears in `comment` blocks within `id.clj` itself. This appears to be a planned feature that was never connected to the rest of the application.

### 2.2 `genegraph.framework.env` — `src/genegraph/framework/env.clj`

A single function `build-environment` that fetches secrets from Google Cloud Secret Manager.

Not required by any other namespace. The only call to `build-environment` is inside a `comment` block in the same file. Applications using the framework currently pass configuration directly (e.g., via `System/getenv`).

---

## Category 3: Top-Level Expressions with Side Effects

These are bare expressions (not inside `defn`, `def`, or `comment`) that execute when the namespace is loaded. None of the results are bound or used.

### 3.1 Two bare Java method calls — `storage/rdf.clj:164–166`

```clojure
(.shortForm names/prefix-mapping "http://purl.obolibrary.org/obo/MONDO_0100038")  ; line 164
(.getNsPrefixMap names/prefix-mapping)                                              ; line 166
```

These evaluate on every namespace load, return values that are immediately discarded, and have no effect. They are REPL test expressions that were not cleaned up. Neither causes an error but both are meaningless in production.

### 3.2 Bare `try/throw/catch` — `kafka.clj:116–120`

```clojure
(try
  (throw (org.apache.kafka.clients.consumer.NoOffsetForPartitionException.
          (TopicPartition. "test" 0)))
  (catch Exception e
    (instance? org.apache.kafka.clients.consumer.InvalidOffsetException e)))
```

Executes at namespace load time, throws and immediately catches an exception, evaluates `instance?`, and discards the result. This is a REPL experiment verifying the exception hierarchy that was never removed. It allocates and throws an exception on every startup.

### 3.3 `Thread/startVirtualThread` — `user.clj:451`

```clojure
(Thread/startVirtualThread #(println "Hello world!"))
```

This is a top-level expression outside any `comment` block in `user.clj`. It starts a virtual thread that prints to stdout every time the `genegraph.user` namespace is loaded (e.g., at REPL startup). The line below it (`(repeat 10 ...)`) is similarly a bare expression whose return value is discarded.

---

## Category 4: Duplicate Definition

### 4.1 `declare-query` macro — defined in both `storage/rdf.clj:215` and `storage/rdf/query.clj:95`

Both definitions are textually identical. `rdf.clj` re-defines the macro that already exists in `rdf/query.clj`. Since `rdf.clj` requires `rdf/query.clj` (as `query`), the version in `rdf.clj` silently shadows it. Users of the macro who require `rdf.clj` get the `rdf.clj` version; users who require only `rdf/query.clj` get that version. One definition should be removed.

---

## Category 5: Disabled Code Left as Stubs

These use the `#_` reader macro (or are clearly commented out). They are not execution risks but create maintenance confusion.

### 5.1 `store-snapshot` / `restore-snapshot` stubs — `storage.clj:128–133`

```clojure
#_(defn store-snapshot [this] ...)
#_(defn restore-snapshot [this] ...)
```

Originally top-level functions that were replaced by protocol methods on each storage type (`RocksDBInstance`, `RDFStore`). The stubs are dead but their presence implies these were once standalone functions.

### 5.2 Nippy freeze/thaw for `RDFResource` — `storage/rdf/types.clj:126–143`

```clojure
#_(nippy/extend-freeze RDFResource ::rdf-resource ...)
#_(nippy/extend-thaw ::rdf-resource ...)
```

Disabled serialization for a type (`RDFResource`) that no longer exists in the codebase. The comment above says "TODO reimplement freeze for Resource", suggesting this was never completed. The active `Model` freeze/thaw below (lines 154–171) is production code and is fine.

### 5.3 Three GCS helper functions — `storage/gcs.clj:275–300`

```clojure
#_(defn channel-write-string! ...)
#_(defn get-files-with-prefix! ...)
#_(defn push-directory-to-bucket! ...)
```

Commented out with a note: "waiting for a use case". These reference a `fs/` namespace that is not imported, so they could not be enabled without additional work.

---

## Borderline Cases (Not Recommended for Removal)

The following are not called within the framework itself but are reasonably part of the public API exposed to downstream application code. They should be retained but documented:

- **`rdf.clj`**: `read-rdf`, `resource`, `model`, `->kw`, `curie`, `ld->`, `ld1->`, `ld->*`, `ld1->*`, `construct-statement`, `statement`, `statements->model`, `blank-node`, `union`, `to-turtle`, `pp-model`, `difference`, `is-isomorphic?`, `is-rdf-type?`, `create-query`, `declare-query` — These form the RDF query/model API layer that applications built on this framework would use. `pp-model`, `to-turtle`, and `difference` in particular appear in `user.clj` REPL sessions.
- **`kafka/admin.clj`**: `admin-actions-by-cluster`, `configure-kafka-for-app!`, `create-admin-client`, `reset-topic` — These are operational tools called interactively (as shown in `app_lifecycle_example.clj`).
- **`gcs.clj`**: `put-file-in-bucket!`, `get-file-from-bucket!` — Utility functions used from REPL; not hooked into the storage protocol but useful for administration.

---

---

## Category 6: Latent Bugs Discovered During Testing

These are bugs in production code found while writing tests. They are not unused code per se but are noted here because the testing process revealed them.

### 6.1 `has-custom-system-processor?` — `app.clj:173`

```clojure
(defn has-custom-system-processor? [app]
  (some #(= :system (:subscribe %)) (:processors app)))
```

`(:processors app)` is a map. Calling `some` on a map iterates over its `MapEntry` objects (2-element vector-like pairs `[key value]`). Calling `(:subscribe map-entry)` on a `MapEntry` returns `nil` (map entries do not implement `ILookup`), so the predicate is always false and `has-custom-system-processor?` always returns `nil`.

As a result, `add-default-system-processor` always adds the default system processor, even when a custom `:subscribe :system` processor is defined. The fix is to use `vals`:

```clojure
(defn has-custom-system-processor? [app]
  (some #(= :system (:subscribe %)) (vals (:processors app))))
```

---

## Recommended Removal Priority

| Priority | Item | File | Lines |
|---|---|---|---|
| High | `vec->key-bytes` (has debug `println`) | `storage/rocksdb.clj` | 20–24 |
| High | Bare `try/throw/catch` (allocates exception at startup) | `kafka.clj` | 116–120 |
| High | `(Thread/startVirtualThread ...)` (spawns thread at load) | `user.clj` | 451 |
| High | `restart-kafka-producer!` (has `swap!` bug, dead) | `kafka.clj` | 798–801 |
| High | `get-or-create-gate` (has `assoc-in` bug, dead) | `processor.clj` | 274–279 |
| Medium | `add-model` multimethod (no implementations) | `event.clj` | 8 |
| Medium | `execute-async` / `execute-sync` | `rdf/instance.clj` | 205–217 |
| Medium | `ignorable-exceptions`, `fatal-exceptions`, `restartable-exceptions` | `kafka.clj` | 50–77 |
| Medium | `in-exception-category?`, `exception->error-map` | `kafka.clj` | 79–94 |
| Medium | Two bare top-level method calls | `storage/rdf.clj` | 164–166 |
| Medium | Duplicate `declare-query` macro | `storage/rdf.clj` | 215–219 |
| Medium | `acl-binding->map` | `kafka/admin.clj` | 112–121 |
| Low | `genegraph.framework.id` (entire namespace) | `id.clj` | all |
| Low | `genegraph.framework.env` (entire namespace) | `env.clj` | all |
| Low | `#_` stub functions | `storage.clj`, `rdf/types.clj`, `gcs.clj` | various |
