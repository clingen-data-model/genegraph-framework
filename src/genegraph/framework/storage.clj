(ns genegraph.framework.storage
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

#_(defn store
  "Add deferred write effect to event"
  [event instance k v]
  (let [commit-promise (promise)]
    (update event
            :effects
            conj
            {:command write
             :args [(get-in event [::storage instance]) k v commit-promise]
             :commit-promise commit-promise})))

(comment
  (store {::storage {:some-storage 'some-storage}} :some-storage :key "value")
  )



;; write (key-value)
;; get (key)
;; query ? (create query object) -- how far does this abstraction go with Jena. Are we going to use it with relational too? JDBC? 
;; delete (key)
;; scan (prefix or range)
;; delete range (prefix or range)

