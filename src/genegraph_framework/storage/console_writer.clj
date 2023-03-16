(ns genegraph-framework.storage.console-writer
  (:require
   [genegraph-framework.protocol :as p]
   [clojure.pprint :refer [pprint]]))

(defrecord ConsoleWriter []
    
  p/StorageWrite
  (write-record [this k v]
    (pprint k)
    (pprint v))

  p/Lifecycle
  (start [_])
  (stop [_])

  p/StorageWrite
  (write [_ _ v] (pprint v)))


(defmethod p/init :console-writer [_]
  (->ConsoleWriter))

(let [w (->ConsoleWriter)]
  (p/effect {::p/storage w ::p/effect :write :value "hi there!"}))

