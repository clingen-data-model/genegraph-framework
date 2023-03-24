(ns genegraph-framework.storage
  (:refer-clojure :exclude [get]))

(defprotocol IndexedWrite
  (write [this k v]))

(defprotocol IndexedRead
  (get [this k]))

(defprotocol IndexedDelete
  (delete [this k]))

(defprotocol RangeRead
  (scan [this prefix] [this begin end]))

(defprotocol RangeDelete
  (range-delete [this prefix] [this begin end]))


;; write (key-value)
;; get (key)
;; query ? (create query object) -- how far does this abstraction go with Jena. Are we going to use it with relational too? JDBC? 
;; delete (key)
;; scan (prefix or range)
;; delete range (prefix or range)

