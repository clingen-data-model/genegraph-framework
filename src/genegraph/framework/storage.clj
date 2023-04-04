(ns genegraph.framework.storage
  (:refer-clojure :exclude [read]))

(defprotocol IndexedWrite
  (write [this k v] [this k v commit-promise]))

(defprotocol IndexedRead
  (read [this k]))

(defprotocol IndexedDelete
  (delete [this k] [this k commit-promise]))

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

