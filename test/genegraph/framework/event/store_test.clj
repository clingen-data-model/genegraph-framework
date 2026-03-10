(ns genegraph.framework.event.store-test
  (:require [genegraph.framework.event.store :as event-store]
            [clojure.test :refer [deftest testing is]])
  (:import [java.io File]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn temp-file
  "Create a temp file that is deleted on JVM exit."
  []
  (doto (File/createTempFile "event-store-test-" ".edn.gz")
    .deleteOnExit))

;; ---------------------------------------------------------------------------
;; with-event-writer / with-event-reader round-trip
;; ---------------------------------------------------------------------------

(deftest writer-reader-roundtrip-test
  (testing "prn inside with-event-writer, then event-seq returns the same maps"
    (let [f      (temp-file)
          events [{:a 1} {:b 2} {:c 3}]]
      (event-store/with-event-writer [_ f]
        (run! prn events))
      (event-store/with-event-reader [r f]
        (is (= events (into [] (event-store/event-seq r)))))))

  (testing "events with complex nested Clojure data round-trip correctly"
    (let [f      (temp-file)
          events [{:nested {:map [1 2 3] :set #{:a :b}} :count 99}
                  {:keyword :val :nil-val nil :vec [true false]}]]
      (event-store/with-event-writer [_ f]
        (run! prn events))
      (event-store/with-event-reader [r f]
        (is (= events (into [] (event-store/event-seq r)))))))

  (testing "event-seq preserves insertion order (FIFO)"
    (let [f      (temp-file)
          events (mapv #(hash-map :n %) (range 10))]
      (event-store/with-event-writer [_ f]
        (run! prn events))
      (event-store/with-event-reader [r f]
        (is (= events (into [] (event-store/event-seq r)))))))

  (testing "each prn call in the writer produces one readable event"
    (let [f (temp-file)]
      (event-store/with-event-writer [_ f]
        (prn {:first 1})
        (prn {:second 2})
        (prn {:third 3}))
      (event-store/with-event-reader [r f]
        (let [result (into [] (event-store/event-seq r))]
          (is (= 3 (count result)))
          (is (= {:first 1}  (nth result 0)))
          (is (= {:second 2} (nth result 1)))
          (is (= {:third 3}  (nth result 2))))))))

;; ---------------------------------------------------------------------------
;; store-events
;; ---------------------------------------------------------------------------

(deftest store-events-test
  (testing "store-events then event-seq returns the written sequence"
    (let [f      (temp-file)
          events [{:hablo :espanol} {:no :ingles}]]
      (event-store/store-events f events)
      (event-store/with-event-reader [r f]
        (is (= events (into [] (event-store/event-seq r)))))))

  (testing "store-events overwrites any previous content in the file"
    (let [f       (temp-file)
          first-  [{:batch :one}]
          second- [{:batch :two} {:more :data}]]
      (event-store/store-events f first-)
      (event-store/store-events f second-)
      (event-store/with-event-reader [r f]
        (is (= second- (into [] (event-store/event-seq r)))))))

  (testing "store-events with an empty sequence writes a readable empty file"
    (let [f (temp-file)]
      (event-store/store-events f [])
      (event-store/with-event-reader [r f]
        (is (= [] (into [] (event-store/event-seq r))))))))

;; ---------------------------------------------------------------------------
;; Edge cases — empty and single-event files
;; ---------------------------------------------------------------------------

(deftest edge-case-test
  (testing "empty writer (no prn calls) yields an empty event-seq"
    (let [f (temp-file)]
      (event-store/with-event-writer [_ f])
      (event-store/with-event-reader [r f]
        (is (= [] (into [] (event-store/event-seq r)))))))

  (testing "file with exactly one event yields a single-element sequence"
    (let [f (temp-file)]
      (event-store/with-event-writer [_ f]
        (prn {:only :event}))
      (event-store/with-event-reader [r f]
        (let [result (into [] (event-store/event-seq r))]
          (is (= 1 (count result)))
          (is (= {:only :event} (first result))))))))

;; ---------------------------------------------------------------------------
;; Gzip compression
;; ---------------------------------------------------------------------------

(deftest gzip-compression-test
  (testing "the file written by with-event-writer starts with the gzip magic bytes"
    ;; GZIP format: first two bytes are always 0x1F 0x8B
    (let [f (temp-file)]
      (event-store/with-event-writer [_ f]
        (prn {:any :event}))
      (let [raw   (java.nio.file.Files/readAllBytes (.toPath f))
            byte0 (bit-and (aget raw 0) 0xFF)
            byte1 (bit-and (aget raw 1) 0xFF)]
        (is (= 0x1F byte0))
        (is (= 0x8B byte1)))))

  (testing "raw file bytes do not reproduce the original event data"
    ;; Gzip encodes the content; reading raw bytes as plain text yields garbage,
    ;; not the original Clojure map.
    (let [f      (temp-file)
          event  {:a 1 :b "hello"}]
      (event-store/with-event-writer [_ f]
        (prn event))
      ;; Attempt to read the raw (compressed) bytes as plain EDN.  The result
      ;; will be some unrelated value (or an exception), never the original map.
      (let [raw-result
            (try
              (with-open [rdr (java.io.FileReader. f)
                          pbr (java.io.PushbackReader. rdr)]
                (clojure.edn/read pbr))
              (catch Exception _ ::unreadable))]
        (is (not= event raw-result))))))
