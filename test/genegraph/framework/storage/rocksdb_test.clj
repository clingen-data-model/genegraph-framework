(ns genegraph.framework.storage.rocksdb-test
  (:require [genegraph.framework.storage.rocksdb :as rocksdb]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.protocol :as p]
            [clojure.test :refer [deftest testing is]])
  (:import [java.util Arrays]
           [com.google.common.primitives Longs]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn temp-path []
  (str (System/getProperty "java.io.tmpdir")
       "/rocksdb-test-" (random-uuid)))

(defmacro with-db
  "Open a fresh RocksDB in a unique temp directory, bind it to `sym`,
  execute `body`, then close and destroy the DB."
  [[sym] & body]
  `(let [path# (temp-path)]
     (try
       (with-open [~sym (rocksdb/open path#)]
         ~@body)
       (finally
         (rocksdb/destroy path#)))))

(defn bytes<
  "True if byte array `a` is lexicographically less than `b`."
  [^bytes a ^bytes b]
  (neg? (Arrays/compare a b)))

(defn bytes=
  "True if byte arrays `a` and `b` have equal contents."
  [^bytes a ^bytes b]
  (Arrays/equals a b))

;; ---------------------------------------------------------------------------
;; Key encoding — k->bytes (pure, no DB needed)
;; ---------------------------------------------------------------------------

(deftest string-key-encoding-test
  (testing "same string produces the same bytes"
    (is (bytes= (rocksdb/k->bytes "hello") (rocksdb/k->bytes "hello"))))

  (testing "different strings produce different bytes"
    (is (not (bytes= (rocksdb/k->bytes "hello") (rocksdb/k->bytes "world")))))

  (testing "string key is encoded as exactly 16 bytes (XX3-128 hash)"
    (is (= 16 (alength (rocksdb/k->bytes "any-string"))))))

(deftest long-key-encoding-test
  (testing "long key is encoded as exactly 8 bytes (big-endian)"
    (is (= 8 (alength (rocksdb/k->bytes 42)))))

  (testing "smaller longs produce lexicographically smaller bytes (enables range scans)"
    (is (bytes< (rocksdb/k->bytes 1) (rocksdb/k->bytes 2)))
    (is (bytes< (rocksdb/k->bytes 100) (rocksdb/k->bytes 1000))))

  (testing "known value encodes to expected big-endian bytes"
    ;; 1L big-endian = [0 0 0 0 0 0 0 1]
    (let [b (rocksdb/k->bytes 1)]
      (is (= 0 (aget b 0)))
      (is (= 1 (aget b 7)))))

  (testing "same long always encodes to the same bytes"
    (is (bytes= (rocksdb/k->bytes 42) (rocksdb/k->bytes 42)))))

(deftest keyword-key-encoding-test
  (testing "keyword encodes identically to its string representation"
    (is (bytes= (rocksdb/k->bytes :foo) (rocksdb/k->bytes (str :foo)))))

  (testing "same keyword always produces the same bytes"
    (is (bytes= (rocksdb/k->bytes :my-key) (rocksdb/k->bytes :my-key))))

  (testing "different keywords produce different bytes"
    (is (not (bytes= (rocksdb/k->bytes :a) (rocksdb/k->bytes :b))))))

(deftest bytes-key-encoding-test
  (testing "byte array key is returned as-is (identity passthrough)"
    (let [raw (byte-array [1 2 3 4])]
      (is (identical? raw (rocksdb/k->bytes raw))))))

(deftest sequential-key-encoding-test
  (testing "keyword elements are encoded as 16 bytes each (XX3-128 hash)"
    (is (= 32 (alength (rocksdb/k->bytes [:a :b]))))
    (is (= 48 (alength (rocksdb/k->bytes [:a :b :c])))))

  (testing "element order matters — [:a :b] differs from [:b :a]"
    (is (not (bytes= (rocksdb/k->bytes [:a :b])
                     (rocksdb/k->bytes [:b :a])))))

  (testing "same vector always produces the same bytes"
    (is (bytes= (rocksdb/k->bytes [:x :y]) (rocksdb/k->bytes [:x :y])))))

;; ---------------------------------------------------------------------------
;; prefix-range-end (pure function)
;; ---------------------------------------------------------------------------

(deftest prefix-range-end-test
  (testing "output has the same length as the input"
    (let [k (rocksdb/k->bytes [:a :b])]
      (is (= (alength k) (alength (rocksdb/prefix-range-end k))))))

  (testing "prefix bytes (all but the last 8) are unchanged"
    (let [k   (rocksdb/k->bytes [:prefix :suffix])
          end (rocksdb/prefix-range-end k)]
      (is (bytes= (Arrays/copyOfRange k 0 8)
                  (Arrays/copyOfRange end 0 8)))))

  (testing "last 8 bytes are incremented by 1"
    (let [k          (rocksdb/k->bytes [:a :b])
          end        (rocksdb/prefix-range-end k)
          len        (alength k)
          suffix-k   (Longs/fromByteArray (Arrays/copyOfRange k (- len 8) len))
          suffix-end (Longs/fromByteArray (Arrays/copyOfRange end (- len 8) len))]
      (is (= (inc suffix-k) suffix-end))))

  (testing "range-end is lexicographically greater than the input key"
    (let [k (rocksdb/k->bytes [:a :b])]
      (is (bytes< k (rocksdb/prefix-range-end k))))))

;; ---------------------------------------------------------------------------
;; Write / Read
;; ---------------------------------------------------------------------------

(deftest write-read-test
  (testing "write then read round-trips a simple value"
    (with-db [db]
      (storage/write db :k :v)
      (is (= :v (storage/read db :k)))))

  (testing "reading a key never written returns ::storage/miss"
    (with-db [db]
      (is (= ::storage/miss (storage/read db :never-written)))))

  (testing "overwriting a key stores the new value"
    (with-db [db]
      (storage/write db :k :first)
      (storage/write db :k :second)
      (is (= :second (storage/read db :k)))))

  (testing "multiple distinct keys are stored independently"
    (with-db [db]
      (storage/write db :a 1)
      (storage/write db :b 2)
      (storage/write db :c 3)
      (is (= 1 (storage/read db :a)))
      (is (= 2 (storage/read db :b)))
      (is (= 3 (storage/read db :c)))))

  (testing "any Nippy-serializable Clojure value round-trips"
    (with-db [db]
      (let [val {:nested {:map [1 2 3]} :set #{:a :b}}]
        (storage/write db :complex val)
        (is (= val (storage/read db :complex))))))

  (testing "write (3-arity) delivers commit-promise with true"
    (with-db [db]
      (let [cp (promise)]
        (storage/write db :k :v cp)
        (is (= true (deref cp 500 :timeout))))))

  (testing "value is written before commit-promise is delivered"
    (with-db [db]
      (let [cp (promise)]
        (storage/write db :k :stored cp)
        @cp
        (is (= :stored (storage/read db :k)))))))

;; ---------------------------------------------------------------------------
;; Delete
;; ---------------------------------------------------------------------------

(deftest delete-test
  (testing "delete removes a written key; subsequent read returns ::storage/miss"
    (with-db [db]
      (storage/write db :k :v)
      (storage/delete db :k)
      (is (= ::storage/miss (storage/read db :k)))))

  (testing "deleting a non-existent key does not throw"
    (with-db [db]
      (is (nil? (storage/delete db :never-written)))))

  (testing "deleting one key does not affect other keys"
    (with-db [db]
      (storage/write db :a 1)
      (storage/write db :b 2)
      (storage/delete db :a)
      (is (= ::storage/miss (storage/read db :a)))
      (is (= 2 (storage/read db :b)))))

  (testing "delete (3-arity) delivers commit-promise with true"
    (with-db [db]
      (let [cp (promise)]
        (storage/write db :k :v)
        (storage/delete db :k cp)
        (is (= true (deref cp 500 :timeout))))))

  (testing "key is absent before commit-promise is delivered"
    (with-db [db]
      (let [cp (promise)]
        (storage/write db :k :v)
        (storage/delete db :k cp)
        @cp
        (is (= ::storage/miss (storage/read db :k)))))))

;; ---------------------------------------------------------------------------
;; Range scan — Long keys
;; ---------------------------------------------------------------------------

(deftest long-range-scan-test
  (testing "scan [start end) returns values for keys in that half-open interval"
    (with-db [db]
      (doseq [n [1 2 3 4 5]]
        (storage/write db (long n) (keyword (str "v" n))))
      ;; keys 1, 2, 3 are in [1, 4); key 4 is excluded
      (is (= [:v1 :v2 :v3] (storage/scan db 1 4)))))

  (testing "scan with equal start and end returns empty"
    (with-db [db]
      (storage/write db 1 :one)
      (is (= [] (storage/scan db 1 1)))))

  (testing "values are returned in ascending key order"
    (with-db [db]
      ;; write in non-sequential order
      (storage/write db 3 :three)
      (storage/write db 1 :one)
      (storage/write db 2 :two)
      (is (= [:one :two :three] (storage/scan db 1 4))))))

;; ---------------------------------------------------------------------------
;; Range scan — prefix (sequential) keys
;; ---------------------------------------------------------------------------

(deftest prefix-scan-test
  (testing "scan with a keyword prefix returns all values under that prefix"
    (with-db [db]
      (storage/write db [:prefix :a] :va)
      (storage/write db [:prefix :b] :vb)
      (storage/write db [:other :c]  :vc)
      (let [result (set (storage/scan db :prefix))]
        (is (contains? result :va))
        (is (contains? result :vb))
        (is (not (contains? result :vc))))))

  (testing "scan with a prefix that matches nothing returns empty"
    (with-db [db]
      (storage/write db [:other :x] :vx)
      (is (= [] (storage/scan db :no-match))))))

;; ---------------------------------------------------------------------------
;; Range delete
;; ---------------------------------------------------------------------------

(deftest range-delete-test
  (testing "range-delete removes all keys under a prefix; non-matching keys survive"
    (with-db [db]
      (storage/write db [:prefix :a] :va)
      (storage/write db [:prefix :b] :vb)
      (storage/write db [:other :c]  :vc)
      (storage/range-delete db :prefix)
      (is (= [] (storage/scan db :prefix)))
      (is (= :vc (storage/read db [:other :c])))))

  (testing "subsequent scan after range-delete returns empty"
    (with-db [db]
      (storage/write db [:p :x] :vx)
      (storage/write db [:p :y] :vy)
      (storage/range-delete db :p)
      (is (= [] (storage/scan db :p)))))

  (testing "range-delete with start/end removes keys in [start, end)"
    (with-db [db]
      (doseq [n [1 2 3 4 5]]
        (storage/write db (long n) (keyword (str "v" n))))
      (storage/range-delete db 2 4)
      (is (= :v1 (storage/read db 1)))
      (is (= ::storage/miss (storage/read db 2)))
      (is (= ::storage/miss (storage/read db 3)))
      (is (= :v4 (storage/read db 4)))
      (is (= :v5 (storage/read db 5))))))

;; ---------------------------------------------------------------------------
;; Topic offset tracking
;; ---------------------------------------------------------------------------

(deftest topic-offset-test
  (testing "store-offset then retrieve-offset round-trips the value"
    (with-db [db]
      (storage/store-offset db :my-topic 42)
      (is (= 42 (storage/retrieve-offset db :my-topic)))))

  (testing "retrieve-offset for an unknown topic returns ::storage/miss"
    (with-db [db]
      (is (= ::storage/miss (storage/retrieve-offset db :unknown-topic)))))

  (testing "store-offset overwrites a previous value"
    (with-db [db]
      (storage/store-offset db :t 10)
      (storage/store-offset db :t 99)
      (is (= 99 (storage/retrieve-offset db :t)))))

  (testing "store-offset (4-arity) delivers commit-promise with true"
    (with-db [db]
      (let [cp (promise)]
        (storage/store-offset db :t 7 cp)
        (is (= true (deref cp 500 :timeout))))))

  (testing "different topic keys are stored independently"
    (with-db [db]
      (storage/store-offset db :topic-a 1)
      (storage/store-offset db :topic-b 2)
      (is (= 1 (storage/retrieve-offset db :topic-a)))
      (is (= 2 (storage/retrieve-offset db :topic-b))))))

;; ---------------------------------------------------------------------------
;; RocksDBInstance — p/init and record structure
;; ---------------------------------------------------------------------------

(deftest rocksdb-instance-init-test
  (testing "p/init :rocksdb returns a RocksDBInstance record"
    (is (instance? genegraph.framework.storage.rocksdb.RocksDBInstance
                   (p/init {:name :test-db :type :rocksdb :path (temp-path)}))))

  (testing "p/init does not open the DB — instance is nil before start"
    (let [r (p/init {:name :test-db :type :rocksdb :path (temp-path)})]
      (is (nil? (storage/instance r)))))

  (testing "RocksDBInstance satisfies HasInstance"
    (is (satisfies? storage/HasInstance
                    (p/init {:name :test-db :type :rocksdb :path (temp-path)}))))

  (testing "RocksDBInstance satisfies Lifecycle"
    (is (satisfies? p/Lifecycle
                    (p/init {:name :test-db :type :rocksdb :path (temp-path)}))))

  (testing "RocksDBInstance satisfies Resetable"
    (is (satisfies? p/Resetable
                    (p/init {:name :test-db :type :rocksdb :path (temp-path)}))))

  (testing "RocksDBInstance satisfies Status"
    (is (satisfies? p/Status
                    (p/init {:name :test-db :type :rocksdb :path (temp-path)}))))

  (testing "p/status before start reports :stopped"
    (let [r (p/init {:name :test-db :type :rocksdb :path (temp-path)})]
      (is (= :stopped (get-in (p/status r) [:status :status])))))

  (testing "p/status includes :name and :path"
    (let [path (temp-path)
          r    (p/init {:name :test-db :type :rocksdb :path path})]
      (is (= :test-db (:name (p/status r))))
      (is (= path (:path (p/status r)))))))

;; ---------------------------------------------------------------------------
;; RocksDBInstance — p/start / p/stop lifecycle
;; ---------------------------------------------------------------------------

(deftest rocksdb-lifecycle-test
  (testing "p/start opens the DB and sets status to :started"
    (let [path (temp-path)
          r    (p/init {:name :test-db :type :rocksdb :path path})]
      (try
        (p/start r)
        (is (some? (storage/instance r)))
        (is (= :started (get-in (p/status r) [:status :status])))
        (finally
          (p/stop r)
          (rocksdb/destroy path)))))

  (testing "p/stop closes the DB and sets status to :stopped"
    (let [path (temp-path)
          r    (p/init {:name :test-db :type :rocksdb :path path})]
      (try
        (p/start r)
        (p/stop r)
        (is (= :stopped (get-in (p/status r) [:status :status])))
        (finally
          (rocksdb/destroy path)))))

  (testing "DB opened via p/start supports write and read"
    (let [path (temp-path)
          r    (p/init {:name :test-db :type :rocksdb :path path})]
      (try
        (p/start r)
        (storage/write (storage/instance r) :k :v)
        (is (= :v (storage/read (storage/instance r) :k)))
        (finally
          (p/stop r)
          (rocksdb/destroy path))))))

;; ---------------------------------------------------------------------------
;; RocksDBInstance — p/reset
;; ---------------------------------------------------------------------------

(deftest rocksdb-reset-test
  (testing "p/reset with no :reset-opts is a no-op and does not throw"
    (let [path (temp-path)
          r    (p/init {:name :test-db :type :rocksdb :path path})]
      (is (nil? (p/reset r)))))

  (testing "p/reset with :destroy-snapshot true and no :snapshot-handle does not throw"
    (let [path (temp-path)
          r    (p/init {:name :test-db :type :rocksdb :path path
                        :reset-opts {:destroy-snapshot true}})]
      ;; :snapshot-handle is absent, so the destroy-snapshot branch short-circuits
      (is (nil? (p/reset r)))))

  (testing "p/reset with {:destroy-snapshot true} destroys the local DB files"
    (let [path (temp-path)
          r    (p/init {:name :test-db :type :rocksdb :path path
                        :reset-opts {:destroy-snapshot true}})]
      ;; Create the DB so the directory exists
      (with-open [_ (rocksdb/open path)])
      (p/reset r)
      ;; After destroyDB the SST files are gone; directory may remain but be empty
      (let [dir   (clojure.java.io/file path)
            files (when (.exists dir) (seq (.listFiles dir)))]
        (is (nil? files))))))
