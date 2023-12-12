(ns genegraph.framework.storage
  (:require [clojure.java.io :as io])
  (:import [java.io ByteArrayInputStream])
  (:refer-clojure :exclude [read]))

(defprotocol HasInstance
  (instance [this]))

(defprotocol IndexedWrite
  (write [this k v] [this k v commit-promise]))

(defprotocol IndexedRead
  (read [this k]))

(defprotocol IndexedDelete
  (delete [this k] [this k commit-promise]))

(defprotocol Transactional
  (begin [this])
  (end [this]))

(defprotocol RangeRead
  (scan [this prefix] [this begin end]))

(defprotocol RangeDelete
  (range-delete [this prefix] [this begin end]))

(defprotocol TopicBackingStore
  (store-offset [this topic offset] [this topic offset commit-promise])
  (retrieve-offset [this topic]))

(defprotocol Snapshot
  (store-snapshot [this storage-handle])
  (restore-snapshot [this storage-handle]))

(defmulti as-handle :type)

(defmethod as-handle :file [def]
  (io/file (:base def) (:path def)))

(defn ->input-stream [source]
  (cond (map? source) (io/input-stream (as-handle source))
        (string? source) (ByteArrayInputStream. (.getBytes source))
        :else (io/input-stream source)))

(comment
  (as-handle {:type :file
              :base "/users/tristan/data"
              :path "edntest.edn"}))
