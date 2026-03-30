(ns genegraph.framework.storage.rocksdb
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.storage :as storage]
            [taoensso.nippy :as nippy]
            [clojure.java.io :as io])
  (:import (org.rocksdb Options ReadOptions RocksDB Slice CompressionType Checkpoint RocksIterator)
           java.util.Arrays
           java.nio.ByteBuffer
           java.nio.file.Path
           [java.io ByteArrayOutputStream File]
           [java.nio ByteBuffer]
           [net.openhft.hashing LongTupleHashFunction]
           [com.google.common.primitives Longs]))

(def ^:private ^LongTupleHashFunction xx128 (LongTupleHashFunction/xx128))

(defn k->hash-bytes
  "Hash a non-Long key to a 16-byte array using XX3-128."
  [k]
  (let [h  (.hashBytes xx128 (.getBytes (str k)))
        bb (ByteBuffer/allocate 16)]
    (.putLong bb (aget h 0))
    (.putLong bb (aget h 1))
    (.array bb)))

(def array-of-bytes-type (Class/forName "[B"))

;; Keys fall into four categories:

(defprotocol KeyBytes
  (k->bytes [k]))

;; 1) Raw byte array -- The assumption is that the specific
;; key is desired, the byte array is simply passed through
;; to RocksDB as a key.
(extend array-of-bytes-type
  KeyBytes
  {:k->bytes identity})

;; 2) String -- 128-bit hashed value encoded as 16 bytes
(extend String
  KeyBytes
  {:k->bytes k->hash-bytes})

;; 3) Long -- An ordered key is desired
;; for the purpose of range scans
(extend Long
  KeyBytes
  {:k->bytes (fn [x]
               (Longs/toByteArray x))})

;; 4) Keyword -- Same as String
(extend clojure.lang.Keyword
  KeyBytes
  {:k->bytes (fn [x] (k->bytes (str x)))})

;; 5) Sequence -- concatenate k->bytes of each element
(extend clojure.lang.Sequential
  KeyBytes
  {:k->bytes (fn [x]
               (let [parts (mapv k->bytes x)
                     total (reduce + 0 (map alength parts))
                     bb    (ByteBuffer/allocate total)]
                 (run! #(.put bb ^bytes %) parts)
                 (.array bb)))})

(defn open [path]
  (io/make-parents path)
  (RocksDB/open (doto (Options.)
                  (.setCreateIfMissing true)
                  (.setCompressionType CompressionType/LZ4_COMPRESSION)) path))

(defn snapshot-path [rocksdb-def]
  (-> rocksdb-def
      :path
      io/file
      .toPath
      .getParent
      (.resolve (str (name (:name rocksdb-def)) "-checkpoint-" (random-uuid)))
      str))

(defn cleanup-checkpoint [path]
  (->> (io/file path)
       file-seq
       reverse
       (run! io/delete-file)))

(defn rocks-store-snapshot [rocksdb-def]
  (let [path (snapshot-path rocksdb-def)]
    (with-open [cp (Checkpoint/create @(:instance rocksdb-def))]
      (.createCheckpoint cp path))
    (storage/store-archive path (:snapshot-handle rocksdb-def))
    (cleanup-checkpoint path)))

(defn rocks-restore-snapshot [{:keys [snapshot-handle path] :as rocksdb-def}]
  (when (and (storage/exists? (storage/as-handle snapshot-handle))
             (not (.exists (io/file path))))
    (p/system-update rocksdb-def {:state :restoring-snapshot})
    (storage/restore-archive path snapshot-handle)))

(defn destroy [path]
  (with-open [opts (Options.)]
    (RocksDB/destroyDB path opts)))

(defrecord RocksDBInstance [name
                            type
                            path
                            state
                            instance]

  storage/HasInstance
  (storage/instance [_] @instance)

  storage/Snapshot
  (store-snapshot [this]
    (rocks-store-snapshot this))
  (restore-snapshot [this]
    (rocks-restore-snapshot this))
  
  p/Lifecycle
  (start [this]
    (io/make-parents path)
    (when (:load-snapshot this)
      (storage/restore-snapshot this))
    (reset! instance (open path))
    (swap! state assoc :status :started)
    this)
  (stop [this]
    (.close @instance)
    (swap! state assoc :status :stopped)
    this)

  p/Resetable
  (reset [this]
    (when-let [opts (:reset-opts this)]
      (when (and (:destroy-snapshot opts) (:snapshot-handle this))
        (-> this :snapshot-handle storage/as-handle storage/delete-handle))
      (destroy path)))

  p/Status
  (status [this]
    {:name name
     :path path
     :status @state}))

(defmethod p/init :rocksdb [db-def]
  (map->RocksDBInstance
   (assoc db-def
          :state (atom {:status :stopped})
          :instance (atom nil))))

(defn prefix-range-end
  "Return the key defining the (exclusive) upper bound of a scan,
  as defined by RANGE-KEY"
  [^bytes range-key]
  (let [key-length (alength range-key)
        key-prefix-idx (- key-length 8)
        bb (ByteBuffer/allocate key-length)]
    (.put bb range-key 0 key-prefix-idx)
    (.put bb
          (-> (Arrays/copyOfRange range-key key-prefix-idx key-length)
              Longs/fromByteArray
              inc
              Longs/toByteArray))
    (.array bb)))



(defn rocks-write! [^RocksDB db k v]
  (.put db
        (k->bytes k)
        (nippy/fast-freeze v)))

(defn rocks-get [db k]
  (if-let [result (.get db (k->bytes k))]
    (nippy/fast-thaw result)
    ::storage/miss))

(deftype RocksRef [db k]
  clojure.lang.IDeref
  (clojure.lang.IDeref/deref [this]
    (nippy/fast-thaw (.get db k))))

(defn range-get
  "Return range of records in RocksDB. May specify a fixed :start and :end,
   or return all records beginning with :prefix. If :return-ref true, return
  RocksRef objects instead of the records in "
  [db {:keys [start end prefix return]}]
  (let [start-key (k->bytes (or start prefix))]
    (with-open [slice (Slice. (if end
                                (k->bytes end)
                                (prefix-range-end start-key)))
                opts (.setIterateUpperBound (ReadOptions.) slice)
                iter (.newIterator db opts)]
      (.seek iter start-key)
      (loop [v (transient [])]
        (if (.isValid iter)
          (let [v1 (conj! v
                          (case return
                            :key (.key iter)
                            :ref (->RocksRef db (.key iter))
                            (nippy/fast-thaw (.value iter))))]
            (.next iter)
            (recur v1))
          (persistent! v))))))

(defn scan
  ([this prefix]
   (range-get this {:prefix prefix}))
  ([this start end]
   (range-get this {:start start :end end})))

(extend RocksDB
  
  storage/IndexedWrite
  {:write
   (fn
     ([this k v]
      (rocks-write! this k v))
     ([this k v commit-promise]
      (rocks-write! this k v)
      (deliver commit-promise true)))}

  storage/IndexedRead
  {:read rocks-get}

  storage/IndexedDelete
  {:delete (fn
             ([this k] (.delete this (k->bytes k)))
             ([this k commit-promise]
              (.delete this (k->bytes k))
              (deliver commit-promise true)))}

  storage/RangeRead
  {:scan scan}

  storage/RangeDelete
  {:range-delete
   (fn
     ([this prefix]
      (let [kb (k->bytes prefix)]
        (.deleteRange this kb (prefix-range-end kb))))
     ([this start end]
      (.deleteRange this
                    (k->bytes start)
                    (k->bytes end))))}

  storage/TopicBackingStore
  {:store-offset
   (fn
     ([this topic offset]
      (rocks-write! this topic offset))
     ([this topic offset commit-promise]
      (rocks-write! this topic offset)
      (deliver commit-promise true)))
   :retrieve-offset
   (fn [this topic]
     (rocks-get this topic))})

(comment 

  (with-open [db (open "/users/tristan/desktop/test-rocks")]
    (storage/write db :test :value)
    (storage/read db :test))

  (with-open [db (open "/users/tristan/desktop/test-rocks")]
    (storage/write db :test :value)
    (storage/delete db :test)
    (storage/read db :test))


  (with-open [db (open "/users/tristan/desktop/test-rocks")]
    (storage/write db 1 :one)
    (storage/write db 2 :two)
    (storage/write db 3 :three)
    (storage/scan db 1 3))

  (with-open [db (open "/users/tristan/desktop/test-rocks")]
    (storage/write db [:one :two] :onetwo)
    (storage/write db [:one :three] :onethree)
    (storage/write db [:four :five] :fourfive)
    (storage/scan db :one))

  (with-open [db (open "/users/tristan/desktop/test-rocks")]
    (storage/write db [127 :two] :onetwo)
    (storage/write db [127 :three] :onethree)
    (storage/write db [4 :five] :fourfive)
    (storage/scan db 127))

  (with-open [db (open "/users/tristan/desktop/test-rocks")]
    (storage/write db [:one :two] :onetwo)
    (storage/write db [:one :three] :onethree)
    (storage/write db [:four :five] :fourfive)
    (mapv deref (range-get db {:prefix :one :return :ref})))

  (with-open [db (open "/users/tristan/desktop/test-rocks")]
    (storage/write db [:one :two] :onetwo)
    (storage/write db [:one :three] :onethree)
    (storage/write db [:four :five] :fourfive)
    (storage/range-delete db :one)
    (storage/scan db :one))

  )





