(ns genegraph-framework.protocol
  "Defines protocols and multimethods for entities in Genegraph")

(defmulti init :type)

(defprotocol Lifecycle
  (start [this])
  (stop [this]))

(defprotocol Queue
  (poll [this])
  (offer [this x]))

(defprotocol StorageWrite
  (write [this k v]))
