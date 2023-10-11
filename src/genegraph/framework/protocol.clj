(ns genegraph.framework.protocol
  "Defines protocols, multimethods, and commonly used functions
  for entities in Genegraph.")

(defmulti init :type)

(defprotocol Lifecycle
  (start [this])
  (stop [this]))

(defprotocol Consumer
  "For topics allowing consumption of records"
  (poll [this] "Returns a value, or nil if poll request times out."))

(defprotocol Publisher
  "For topics allowing publication of records"
  (publish [this event] "Publish event to topic"))

(defprotocol Offsets
  "For topics supporting offsets"
  (set-offset! [this offset] "Set local offset for topic"))

(defn exception
  "Registers exception e occurred in entity this"
  [this e]
  (println "exception " (type e) " in " (:name this) )
  (swap! (:state this)
         assoc
         :status :exception
         :exception e))
