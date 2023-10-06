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
  (offset [this] "Local offset for topic")
  (last-available-offset [this] "Last available offset for topic")
  (last-committed-offset [this]
    "Last offset committed for this topic, e.g. the current
consumer group offset.")
  (set-offset! [this offset] "Set local offset for topic")
  (committed-offset [this] "Committed offset for topic."))

(defn exception
  "Registers exception e occurred in entity this"
  [this e]
  (swap! (:state this)
         assoc
         :status :exception
         :exception e))
