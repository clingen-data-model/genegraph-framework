(ns genegraph.framework.storage.rocksdb
  (:require [genegraph.framework.protocol :as p]
            [genegraph.framework.storage :as storage]
            [taoensso.nippy :as nippy]
            [clojure.java.io :as io]
            [digest])
  (:import (org.rocksdb Options ReadOptions RocksDB Slice CompressionType)
           java.util.Arrays
           java.nio.ByteBuffer
           java.io.ByteArrayOutputStream))



(defn open [path]
  (io/make-parents path)
  (RocksDB/open (doto (Options.)
                  (.setCreateIfMissing true)
                  (.setCompressionType CompressionType/LZ4_COMPRESSION)) path))

(defrecord RocksDBInstance [name
                            type
                            path
                            state
                            instance]
  p/Lifecycle
  (start [this]
    (io/make-parents path)
    (reset! instance
            (open path))
    this)
  (stop [this]
    (.close @instance)
    this))

(defmethod p/init :rocksdb [db-def]
  (map->RocksDBInstance
   (assoc db-def
          :state (atom :stopped)
          :instance (atom nil))))

(def array-of-bytes-type (Class/forName "[B"))

(defmulti key-to-byte-array class)

(defmethod key-to-byte-array array-of-bytes-type [k] k)

(defmethod key-to-byte-array java.lang.Long [k]
  (-> (ByteBuffer/allocate (java.lang.Long/BYTES))
      (.putLong k)
      .array))

(defmethod key-to-byte-array clojure.lang.PersistentVector [k]
  (let [bs (ByteArrayOutputStream.)]
    (run! #(.writeBytes bs %) (map key-to-byte-array k))
    (.toByteArray bs)))

(defmethod key-to-byte-array :default [k]
  (-> k nippy/fast-freeze digest/md5 .getBytes))

(comment
  (key-to-byte-array 123456788)
  (key-to-byte-array "this")
  (key-to-byte-array {:an :object})
  (key-to-byte-array [1 2 3])
  )

(defn prefix-range-end
  "Return the key defining the (exclusive) upper bound of a scan,
  as defined by RANGE-KEY"
  [^bytes range-key]
  (let [last-byte-idx (dec (alength range-key))]
    (doto (Arrays/copyOf range-key (alength range-key))
      (aset-byte last-byte-idx (inc (aget range-key last-byte-idx))))))

(comment
  (prefix-range-end (key-to-byte-array 3))
  )

(defn destroy [path]
  (with-open [opts (Options.)]
    (RocksDB/destroyDB path opts)))

(defn range-get
  "In order to limit the potential for memory leaks, this returns a
  PersistentVector and handles all the lifecycle operations for the
  required RocksDB entities."
  ([db prefix]
   (let [ba (key-to-byte-array prefix)]
     (range-get db ba (prefix-range-end ba))))
  ([db start end]
   (with-open [slice (Slice. (key-to-byte-array end))
               opts (.setIterateUpperBound (ReadOptions.) slice)
               iter (.newIterator db opts)]
     (.seek iter (key-to-byte-array start))
     (loop [v (transient [])]
       (if (.isValid iter)
         (let [v1 (conj! v (nippy/fast-thaw (.value iter)))]
           (.next iter)
           (recur v1))
         (persistent! v))))))

(defn rocks-write! [^RocksDB db k v]
  (.put db
        (key-to-byte-array k)
        (nippy/fast-freeze v)))

(defn rocks-get [db k]
  (if-let [result (.get db (key-to-byte-array k))]
    (nippy/fast-thaw result)
    ::storage/miss))

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
  {:delete (fn [this k] (.delete this (key-to-byte-array k)))}

  storage/RangeRead
  {:scan range-get}

  storage/RangeDelete
  {:range-delete
   (fn
     ([this prefix]
      (let [kb (key-to-byte-array prefix)]
        (.deleteRange this kb (prefix-range-end kb))))
     ([this start end]
      (.deleteRange this
                    (key-to-byte-array start)
                    (key-to-byte-array end))))}

  storage/TopicBackingStore
  {:store-offset
   (fn [this topic offset]
     (println "rocks storing offset " topic "-" offset)
     (rocks-write! this topic offset))
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
    (storage/write db [:one :two] :onetwo)
    (storage/write db [:one :three] :onethree)
    (storage/write db [:four :five] :fourfive)
    (storage/range-delete db :one)
    (storage/scan db :one))
  )


