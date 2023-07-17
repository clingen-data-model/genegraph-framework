(ns genegraph.framework.storage
  (:require [clojure.java.io :as io])
  (:refer-clojure :exclude [read]))

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
  (store-offset [this topic offset])
  (retrieve-offset [this topic]))

(defmulti as-handle :type)

(defmethod as-handle :file [def]
  (io/file (:base def) (:path def)))

(comment
  (as-handle {:type :file
              :base "/users/tristan/data"
              :path "edntest.edn"}))

;; write (key-value)
;; get (key)
;; query ? (create query object) -- how far does this abstraction go with Jena. Are we going to use it with relational too? JDBC? 
;; delete (key)
;; scan (prefix or range)
;; delete range (prefix or range)

